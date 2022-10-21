from typing import TYPE_CHECKING, Optional

from dvc.exceptions import UploadError

from ..utils import glob_targets
from . import locked

if TYPE_CHECKING:
    from dvc_objects.db.base import ObjectDB


def _push_worktree(repo, remote):
    from dvc_data.index import checkout
    from dvc_data.index.save import build_tree

    index = repo.index.data["repo"]
    checkout(index, remote.path, remote.fs)

    for stage in repo.index.stages:
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

    return len(index)


@locked
def push(
    self,
    targets=None,
    jobs=None,
    remote=None,
    all_branches=False,
    with_deps=False,
    all_tags=False,
    recursive=False,
    all_commits=False,
    run_cache=False,
    revs=None,
    glob=False,
    odb: Optional["ObjectDB"] = None,
    include_imports=False,
):
    _remote = self.cloud.get_remote(name=remote)
    if _remote.worktree:
        return _push_worktree(self, _remote)

    used_run_cache = (
        self.stage_cache.push(remote, odb=odb) if run_cache else []
    )

    if isinstance(targets, str):
        targets = [targets]

    expanded_targets = glob_targets(targets, glob=glob)

    used = self.used_objs(
        expanded_targets,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
        with_deps=with_deps,
        force=True,
        remote=remote,
        jobs=jobs,
        recursive=recursive,
        used_run_cache=used_run_cache,
        revs=revs,
    )

    pushed = len(used_run_cache)
    if odb:
        all_ids = set()
        for dest_odb, obj_ids in used.items():
            if not include_imports and dest_odb and dest_odb.read_only:
                continue
            all_ids.update(obj_ids)
        result = self.cloud.push(all_ids, jobs, remote=remote, odb=odb)
        if result.failed:
            raise UploadError(len(result.failed))
        pushed += len(result.transferred)
    else:
        for dest_odb, obj_ids in used.items():
            if dest_odb and dest_odb.read_only:
                continue
            result = self.cloud.push(
                obj_ids, jobs, remote=remote, odb=odb or dest_odb
            )
            if result.failed:
                raise UploadError(len(result.failed))
            pushed += len(result.transferred)
    return pushed
