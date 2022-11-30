import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

from dvc.fs.callbacks import Callback

if TYPE_CHECKING:
    from dvc.cloud import Remote
    from dvc.index import Index, IndexView
    from dvc.output import Output
    from dvc.repo import Repo
    from dvc.stage import Stage
    from dvc.types import TargetType
    from dvc_data.hashfile.meta import Meta

logger = logging.getLogger(__name__)


def worktree_view(
    index: "Index",
    targets: Optional["TargetType"] = None,
    push: bool = False,
    latest_only: bool = True,
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
        # If we are not enforcing push to latest version and have a version
        # for this out, we assume it still exists and can skip pushing it
        if push and not latest_only and out.meta.version_id is not None:
            return False
        return True

    return index.targets_view(
        targets,
        stage_filter=stage_filter,
        outs_filter=outs_filter,
        **kwargs,
    )


def fetch_worktree(repo: "Repo", remote: "Remote") -> int:
    from dvc_data.index import save

    view = worktree_view(repo.index)
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
        return save(index, callback=cb)


def push_worktree(repo: "Repo", remote: "Remote") -> int:
    from dvc_data.index import build, checkout

    view = worktree_view(repo.index, push=True, latest_only=remote.worktree)
    new_index = view.data["repo"]
    if remote.worktree:
        logger.debug("Indexing latest worktree for '%s'", remote.path)
        old_index = build(remote.path, remote.fs)
        logger.debug("Pushing worktree changes to '%s'", remote.path)
    else:
        old_index = None
        logger.debug("Pushing version-aware files to '%s'", remote.path)

    if remote.worktree:

        # for files, if our version's checksum (etag) matches the latest remote
        # checksum, we do not need to push, even if the version IDs don't match
        def _checksum(meta: "Meta") -> Any:
            if not meta or meta.isdir:
                return meta
            return getattr(meta, remote.fs.PARAM_CHECKSUM)

        diff_kwargs: Dict[str, Any] = {
            "meta_only": True,
            "meta_cmp_key": _checksum,
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
            **diff_kwargs,
        )
    if pushed:
        _update_pushed_meta(repo, view)
    return pushed


def _update_pushed_meta(repo: "Repo", view: "IndexView"):
    from dvc_data.index.save import build_tree

    for stage in view.stages:
        for out in stage.outs:
            workspace, key = out.index_key
            index = repo.index.data[workspace]
            entry = index[key]
            if out.isdir():
                old_tree = out.get_obj()
                entry.hash_info = old_tree.hash_info
                entry.meta = out.meta
                for subkey, entry in index.iteritems(key):
                    if entry.meta.isdir:
                        continue
                    fs_path = repo.fs.path.join(repo.root_dir, *subkey)
                    meta, hash_info = old_tree.get(
                        repo.fs.path.relparts(fs_path, out.fs_path)
                    )
                    entry.hash_info = hash_info
                    if meta.version_id is not None:
                        # preserve existing version IDs for unchanged files in
                        # this dir (entry will have the latest remote version
                        # ID after checkout)
                        entry.meta = meta
                tree_meta, new_tree = build_tree(index, key)
                out.obj = new_tree
                out.hash_info = new_tree.hash_info
                out.meta = tree_meta
            else:
                out.hash_info = entry.hash_info
                if out.meta.version_id is None:
                    out.meta = entry.meta
        stage.dvcfile.dump(stage, with_files=True, update_pipeline=False)
