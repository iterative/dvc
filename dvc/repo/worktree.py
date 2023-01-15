import logging
from functools import partial
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Union, cast

from dvc.fs.callbacks import Callback

if TYPE_CHECKING:
    from dvc.data_cloud import Remote
    from dvc.output import Output
    from dvc.repo import Repo
    from dvc.repo.index import Index, IndexView
    from dvc.repo.stage import StageInfo
    from dvc.stage import Stage
    from dvc.types import TargetType
    from dvc_data.hashfile.meta import Meta
    from dvc_data.hashfile.tree import Tree
    from dvc_data.index import DataIndex, DataIndexView
    from dvc_objects.fs.base import FileSystem

logger = logging.getLogger(__name__)


# for files, if our version's checksum (etag) matches the latest remote
# checksum, we do not need to push, even if the version IDs don't match
def _meta_checksum(fs: "FileSystem", meta: "Meta") -> Any:
    if not meta or meta.isdir:
        return meta
    return getattr(meta, cast(str, fs.PARAM_CHECKSUM))


def worktree_view(
    index: "Index",
    targets: Optional["TargetType"] = None,
    push: bool = False,
    **kwargs,
) -> "IndexView":
    """Return view of data that can be stored in worktree remotes.

    Args:
        targets: Optional targets.
        push: Whether the view should be restricted to pushable data only.

    Additional kwargs will be passed into target collection.
    """

    def stage_filter(stage: "Stage") -> bool:
        if push and stage.is_repo_import:
            return False
        return True

    def outs_filter(out: "Output") -> bool:
        if (
            not out.is_in_repo
            or not out.use_cache
            or (push and not out.can_push)
        ):
            return False
        return True

    return index.targets_view(
        targets,
        stage_filter=stage_filter,
        outs_filter=outs_filter,
        **kwargs,
    )


def fetch_worktree(
    repo: "Repo",
    remote: "Remote",
    targets: Optional["TargetType"] = None,
    jobs: Optional[int] = None,
    **kwargs,
) -> int:
    from dvc_data.index import save

    view = worktree_view(repo.index, targets=targets, **kwargs)
    index = view.data["repo"]
    for key, entry in index.iteritems():
        entry.fs = remote.fs
        entry.path = remote.fs.path.join(
            remote.path,
            *key,
        )
    total = len(index)
    with Callback.as_tqdm_callback(
        unit="file", desc="Fetch", disable=total == 0
    ) as cb:
        cb.set_size(total)
        return save(index, callback=cb, jobs=jobs)


def push_worktree(
    repo: "Repo",
    remote: "Remote",
    targets: Optional["TargetType"] = None,
    jobs: Optional[int] = None,
    **kwargs,
) -> int:
    from dvc.repo.index import build_data_index
    from dvc_data.index import checkout

    view = worktree_view(
        repo.index,
        push=True,
        targets=targets,
        **kwargs,
    )
    new_index = view.data["repo"]
    if remote.worktree:
        logger.debug("indexing latest worktree for '%s'", remote.path)
        old_index = build_data_index(view, remote.path, remote.fs)
        logger.debug("Pushing worktree changes to '%s'", remote.path)
    else:
        old_index = None
        logger.debug("Pushing version-aware files to '%s'", remote.path)

    if remote.worktree:
        diff_kwargs: Dict[str, Any] = {
            "meta_only": True,
            "meta_cmp_key": partial(_meta_checksum, remote.fs),
        }
    else:
        diff_kwargs = {}

    total = len(new_index)
    with Callback.as_tqdm_callback(
        unit="file", desc="Push", disable=total == 0
    ) as cb:
        cb.set_size(total)
        pushed = checkout(
            new_index,
            remote.path,
            remote.fs,
            old=old_index,
            delete=remote.worktree,
            callback=cb,
            latest_only=remote.worktree,
            jobs=jobs,
            **diff_kwargs,
        )
    if pushed:
        for stage in view.stages:
            for out in stage.outs:
                workspace, _key = out.index_key
                _update_out_meta(out, repo.index.data[workspace])
            stage.dump(with_files=True, update_pipeline=False)
    return pushed


def _update_out_meta(
    out: "Output", index: Union["DataIndex", "DataIndexView"]
):
    from dvc_data.index.save import build_tree

    _, key = out.index_key
    entry = index[key]
    repo = out.stage.repo
    if out.isdir():
        old_tree = cast("Tree", out.get_obj())
        entry.hash_info = old_tree.hash_info
        entry.meta = out.meta
        for subkey, entry in index.iteritems(key):
            if entry.meta is not None and entry.meta.isdir:
                continue
            fs_path = repo.fs.path.join(repo.root_dir, *subkey)
            meta, hash_info = old_tree.get(
                repo.fs.path.relparts(fs_path, out.fs_path)
            ) or (None, None)
            entry.hash_info = hash_info
            if meta is not None and meta.version_id is not None:
                # preserve existing version IDs for unchanged files in
                # this dir (entry will have the latest remote version
                # ID after checkout)
                entry.meta = meta
        tree_meta, new_tree = build_tree(index, key)
        out.obj = new_tree
        out.hash_info = new_tree.hash_info
        out.meta = tree_meta
    else:
        if entry.hash_info:
            out.hash_info = entry.hash_info
        if out.meta.version_id is None:
            out.meta = entry.meta


def update_worktree_stages(
    repo: "Repo",
    stage_infos: Iterable["StageInfo"],
    remote: "Remote",
):
    from dvc.repo.index import IndexView
    from dvc_data.index import build

    def outs_filter(out: "Output") -> bool:
        return out.is_in_repo and out.use_cache and out.can_push

    view = IndexView(
        repo.index,
        stage_infos,
        outs_filter=outs_filter,
    )
    local_index = view.data["repo"]
    logger.debug("indexing latest worktree for '%s'", remote.path)
    remote_index = build(remote.path, remote.fs)
    for stage in view.stages:
        for out in stage.outs:
            _workspace, key = out.index_key
            if key not in remote_index:
                logger.warning(
                    "Could not update '%s', it does not exist in the remote",
                    out,
                )
                continue
            entry = remote_index[key]
            if (
                entry.meta
                and entry.meta.isdir
                and not any(
                    subkey != key and subentry.meta and not subentry.meta.isdir
                    for subkey, subentry in remote_index.iteritems(key)
                )
            ):
                logger.warning(
                    "Could not update '%s', directory is empty in the "
                    "remote",
                    out,
                )
                continue
            _fetch_out_changes(out, local_index, remote_index, remote)
        stage.save(merge_versioned=True)
        for out in stage.outs:
            _update_out_meta(out, remote_index)
        stage.dump(with_files=True, update_pipeline=False)


def _fetch_out_changes(
    out: "Output",
    local_index: Union["DataIndex", "DataIndexView"],
    remote_index: Union["DataIndex", "DataIndexView"],
    remote: "Remote",
):
    from dvc_data.index import DataIndex, checkout

    _, key = out.index_key
    old = DataIndex()
    new = DataIndex()
    for _, entry in local_index.iteritems(key):
        old.add(entry)
    for _, entry in remote_index.iteritems(key):
        new.add(entry)
    total = len(new)
    with Callback.as_tqdm_callback(
        unit="file", desc=f"Updating '{out}'", disable=total == 0
    ) as cb:
        cb.set_size(total)
        checkout(
            new,
            out.repo.root_dir,
            out.fs,
            old=old,
            delete=True,
            update_meta=False,
            meta_only=True,
            meta_cmp_key=partial(_meta_checksum, remote.fs),
            callback=cb,
        )
