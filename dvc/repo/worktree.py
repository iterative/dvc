import logging
from functools import partial
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Set, Tuple, Union

from funcy import first

from dvc.exceptions import DvcException
from dvc.fs.callbacks import Callback
from dvc.stage.exceptions import StageUpdateError

if TYPE_CHECKING:
    from dvc.data_cloud import Remote
    from dvc.output import Output
    from dvc.repo import Repo
    from dvc.repo.index import Index, IndexView
    from dvc.repo.stage import StageInfo
    from dvc.stage import Stage
    from dvc.types import TargetType
    from dvc_data.hashfile.meta import Meta
    from dvc_data.index import DataIndex, DataIndexView
    from dvc_objects.fs.base import FileSystem

logger = logging.getLogger(__name__)


# for files, if our version's checksum (etag) matches the latest remote
# checksum, we do not need to push, even if the version IDs don't match
def _meta_checksum(fs: "FileSystem", meta: "Meta") -> Any:
    if not meta or meta.isdir:
        return meta
    assert fs.PARAM_CHECKSUM
    return getattr(meta, fs.PARAM_CHECKSUM)


def worktree_view_by_remotes(
    index: "Index",
    targets: Optional["TargetType"] = None,
    push: bool = False,
    **kwargs: Any,
) -> Iterable[Tuple[Optional[str], "IndexView"]]:
    # pylint: disable=protected-access

    from dvc.repo.index import IndexView

    def outs_filter(view: "IndexView", remote: Optional[str]):
        def _filter(out: "Output") -> bool:
            if out.remote != remote:
                return False
            if view._outs_filter:
                return view._outs_filter(out)
            return True

        return _filter

    view = worktree_view(index, targets=targets, push=push, **kwargs)
    remotes = {out.remote for out in view.outs}

    if len(remotes) <= 1:
        yield first(remotes), view
        return

    for remote in remotes:
        yield remote, IndexView(index, view._stage_infos, outs_filter(view, remote))


def worktree_view(
    index: "Index",
    targets: Optional["TargetType"] = None,
    push: bool = False,
    **kwargs: Any,
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
        if not out.is_in_repo or not out.use_cache or (push and not out.can_push):
            return False
        return True

    return index.targets_view(
        targets,
        stage_filter=stage_filter,
        outs_filter=outs_filter,
        **kwargs,
    )


def _get_remote(
    repo: "Repo", name: Optional[str], default: "Remote", command: str
) -> "Remote":
    if name in (None, default.name):
        return default
    return repo.cloud.get_remote(name, command)


def push_worktree(
    repo: "Repo",
    remote: "Remote",
    targets: Optional["TargetType"] = None,
    jobs: Optional[int] = None,
    **kwargs: Any,
) -> int:
    from dvc.repo.index import build_data_index
    from dvc_data.index.checkout import VersioningNotSupported, apply, compare

    pushed = 0
    stages: Set["Stage"] = set()

    for remote_name, view in worktree_view_by_remotes(
        repo.index, push=True, targets=targets, **kwargs
    ):
        remote_obj = _get_remote(repo, remote_name, remote, "push")
        new_index = view.data["repo"]
        if remote_obj.worktree:
            logger.debug("indexing latest worktree for '%s'", remote_obj.path)
            old_index = build_data_index(view, remote_obj.path, remote_obj.fs)
            logger.debug("Pushing worktree changes to '%s'", remote_obj.path)
        else:
            old_index = None
            logger.debug("Pushing version-aware files to '%s'", remote_obj.path)

        if remote_obj.worktree:
            diff_kwargs: Dict[str, Any] = {
                "meta_only": True,
                "meta_cmp_key": partial(_meta_checksum, remote_obj.fs),
            }
        else:
            diff_kwargs = {}

        with Callback.as_tqdm_callback(
            unit="entry",
            desc=f"Comparing indexes for remote {remote_obj.name!r}",
        ) as cb:
            diff = compare(
                old_index,
                new_index,
                callback=cb,
                delete=remote_obj.worktree,
                **diff_kwargs,
            )

        total = len(new_index)
        with Callback.as_tqdm_callback(
            unit="file",
            desc=f"Pushing to remote {remote_obj.name!r}",
            disable=total == 0,
        ) as cb:
            cb.set_size(total)
            try:
                apply(
                    diff,
                    remote_obj.path,
                    remote_obj.fs,
                    callback=cb,
                    latest_only=remote_obj.worktree,
                    jobs=jobs,
                )
                pushed += len(diff.files_create)
            except VersioningNotSupported:
                logger.exception("")
                raise DvcException(
                    f"remote {remote_obj.name!r} does not support versioning"
                ) from None

        if remote_obj.index is not None:
            for key, entry in new_index.iteritems():
                remote_obj.index[key] = entry
            remote_obj.index.commit()

        for out in view.outs:
            workspace, _key = out.index_key
            _merge_push_meta(out, repo.index.data[workspace], remote_obj.name)
            stages.add(out.stage)

    for stage in stages:
        stage.dump(with_files=True, update_pipeline=False)
    return pushed


def _merge_push_meta(
    out: "Output",
    index: Union["DataIndex", "DataIndexView"],
    remote: Optional[str] = None,
):
    """Merge existing output meta with newly pushed meta.

    Existing version IDs for unchanged files will be preserved to reduce merge
    conflicts (i.e. the DVC output's version ID may not match the pushed/latest
    version ID as long when the file content of both versions is the same).
    """
    from dvc_data.hashfile.tree import Tree
    from dvc_data.index.save import build_tree

    _, key = out.index_key
    entry = index[key]
    repo = out.stage.repo
    if out.isdir():
        old_tree = out.get_obj()
        assert isinstance(old_tree, Tree)
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
            if entry.meta:
                entry.meta.remote = remote
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
    if out.meta:
        out.meta.remote = remote


def update_worktree_stages(
    repo: "Repo",
    stage_infos: Iterable["StageInfo"],
):
    from dvc.repo.index import IndexView

    def outs_filter(out: "Output") -> bool:
        return out.is_in_repo and out.use_cache and out.can_push

    view = IndexView(
        repo.index,
        stage_infos,
        outs_filter=outs_filter,
    )
    local_index = view.data["repo"]
    remote_indexes: Dict[str, Tuple["Remote", "DataIndex"]] = {}
    for stage in view.stages:
        for out in stage.outs:
            _update_worktree_out(repo, out, local_index, remote_indexes)
        stage.dump(with_files=True, update_pipeline=False)


def _update_worktree_out(
    repo: "Repo",
    out: "Output",
    local_index: Union["DataIndex", "DataIndexView"],
    remote_indexes: Dict[str, Tuple["Remote", "DataIndex"]],
):
    from dvc_data.index import build

    remote_name = out.remote or out.meta.remote
    if not remote_name:
        logger.warning(
            "Could not update '%s', it was never pushed to a remote",
            out,
        )
        return

    if remote_name in remote_indexes:
        remote, remote_index = remote_indexes[remote_name]
    else:
        remote = repo.cloud.get_remote(remote_name, "update")
        if not remote.worktree:
            raise StageUpdateError(out.stage.relpath)
        logger.debug("indexing latest worktree for '%s'", remote.path)
        remote_index = build(remote.path, remote.fs)
        remote_indexes[remote_name] = remote, remote_index
    _workspace, key = out.index_key
    if key not in remote_index:
        logger.warning(
            "Could not update '%s', it does not exist in the remote",
            out,
        )
        return

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
            "Could not update '%s', directory is empty in the remote",
            out,
        )
        return

    _fetch_out_changes(out, local_index, remote_index, remote)
    _update_out_meta(
        repo,
        out,
        local_index,
        remote_index,
        remote,
    )


def _fetch_out_changes(
    out: "Output",
    local_index: Union["DataIndex", "DataIndexView"],
    remote_index: Union["DataIndex", "DataIndexView"],
    remote: "Remote",
):
    from dvc_data.index.checkout import apply, compare

    old, new = _get_diff_indexes(out, local_index, remote_index)

    with Callback.as_tqdm_callback(
        unit="entry",
        desc="Comparing indexes",
    ) as cb:
        diff = compare(
            old,
            new,
            delete=True,
            meta_only=True,
            meta_cmp_key=partial(_meta_checksum, remote.fs),
            callback=cb,
        )

    total = len(new)
    with Callback.as_tqdm_callback(
        unit="file", desc=f"Updating '{out}'", disable=total == 0
    ) as cb:
        cb.set_size(total)
        apply(
            diff,
            out.repo.root_dir,
            out.fs,
            update_meta=False,
            storage="data",
            callback=cb,
        )
        out.save()


def _get_diff_indexes(
    out: "Output",
    local_index: Union["DataIndex", "DataIndexView"],
    remote_index: Union["DataIndex", "DataIndexView"],
) -> Tuple["DataIndex", "DataIndex"]:
    from dvc_data.index import DataIndex

    _, key = out.index_key
    old = DataIndex()
    new = DataIndex()
    for _, entry in local_index.iteritems(key):
        old.add(entry)
    for _, entry in remote_index.iteritems(key):
        new.add(entry)

    for prefix, storage in local_index.storage_map.items():
        old.storage_map[prefix] = storage

    for prefix, storage in remote_index.storage_map.items():
        new.storage_map[prefix] = storage

    return old, new


def _update_out_meta(
    repo: "Repo",
    out: "Output",
    local_index: Union["DataIndex", "DataIndexView"],
    remote_index: Union["DataIndex", "DataIndexView"],
    remote: "Remote",
):
    from dvc_data.index.save import build_tree

    index = _get_update_diff_index(repo, out, local_index, remote_index, remote)

    _, key = out.index_key
    entry = index[key]
    if out.isdir():
        tree_meta, new_tree = build_tree(index, key)
        out.obj = new_tree
        out.hash_info = new_tree.hash_info
        out.meta = tree_meta
    else:
        if entry.hash_info:
            out.hash_info = entry.hash_info
        out.meta = entry.meta
    if out.meta:
        out.meta.remote = remote.name


def _get_update_diff_index(
    repo: "Repo",
    out: "Output",
    local_index: Union["DataIndex", "DataIndexView"],
    remote_index: Union["DataIndex", "DataIndexView"],
    remote: "Remote",
) -> "DataIndex":
    from dvc_data.hashfile.tree import Tree
    from dvc_data.index import DataIndex
    from dvc_data.index.diff import ADD, MODIFY, UNCHANGED, diff

    old, new = _get_diff_indexes(out, local_index, remote_index)
    index = DataIndex()
    for change in diff(
        old,
        new,
        meta_only=True,
        meta_cmp_key=partial(_meta_checksum, remote.fs),
        with_unchanged=True,
    ):
        if change.typ in (ADD, MODIFY):
            entry = change.new
            # preserve md5's which were calculated in out.save() after
            # downloading
            if out.isdir():
                if not entry.meta.isdir:
                    fs_path = repo.fs.path.join(repo.root_dir, *entry.key)
                    tree = out.obj
                    assert isinstance(tree, Tree)
                    _, entry.hash_info = tree.get(  # type: ignore[misc]
                        repo.fs.path.relparts(fs_path, out.fs_path)
                    )
            else:
                entry.hash_info = out.hash_info
            index[change.new.key] = change.new
        elif change.typ == UNCHANGED:
            index[change.old.key] = change.old
    return index
