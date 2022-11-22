from typing import TYPE_CHECKING, Optional

from dvc.fs.callbacks import Callback

if TYPE_CHECKING:
    from dvc.cloud import Remote
    from dvc.index import Index, IndexView
    from dvc.output import Output
    from dvc.repo import Repo
    from dvc.stage import Stage
    from dvc.types import TargetType


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
    from dvc_data.index import checkout
    from dvc_data.index.save import build_tree

    view = worktree_view(repo.index, push=True)
    index = view.data["repo"]
    total = len(index)
    with Callback.as_tqdm_callback(
        unit="file", desc="Push", disable=total == 0
    ) as cb:
        cb.set_size(total)
        pushed = checkout(
            index,
            remote.path,
            remote.fs,
            latest_only=False,
            callback=cb,
        )

    for stage in view.stages:
        for out in stage.outs:
            if not out.use_cache:
                continue

            if not out.is_in_repo:
                continue

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
                    _, hash_info = old_tree.get(
                        repo.fs.path.relparts(fs_path, out.fs_path)
                    )
                    entry.hash_info = hash_info
                tree_meta, new_tree = build_tree(index, key)
                out.obj = new_tree
                out.hash_info = new_tree.hash_info
                out.meta = tree_meta
            else:
                out.hash_info = entry.hash_info
                out.meta = entry.meta
        stage.dvcfile.dump(stage, with_files=True, update_pipeline=False)

    return pushed
