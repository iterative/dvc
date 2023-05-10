from contextlib import suppress
from typing import TYPE_CHECKING, Optional, Sequence

from dvc.config import NoRemoteError
from dvc.exceptions import InvalidArgumentError, UploadError
from dvc.utils import glob_targets

from . import locked

if TYPE_CHECKING:
    from dvc.data_cloud import Remote
    from dvc.repo import Repo
    from dvc.types import TargetType
    from dvc_objects.db import ObjectDB


@locked
def push(  # noqa: C901, PLR0913
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
    worktree_remote: Optional["Remote"] = None
    with suppress(NoRemoteError):
        _remote = self.cloud.get_remote(name=remote)
        if _remote and (_remote.worktree or _remote.fs.version_aware):
            worktree_remote = _remote

    pushed = 0
    used_run_cache = self.stage_cache.push(remote, odb=odb) if run_cache else []
    pushed += len(used_run_cache)

    if isinstance(targets, str):
        targets = [targets]

    expanded_targets = glob_targets(targets, glob=glob)

    if worktree_remote is not None:
        pushed += _push_worktree(
            self,
            worktree_remote,
            revs=revs,
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
            targets=expanded_targets,
            jobs=jobs,
            with_deps=with_deps,
            recursive=recursive,
        )
    else:
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
            push=True,
        )

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


def _push_worktree(
    repo: "Repo",
    remote: "Remote",
    revs: Optional[Sequence[str]] = None,
    all_branches: bool = False,
    all_tags: bool = False,
    all_commits: bool = False,
    targets: Optional["TargetType"] = None,
    jobs: Optional[int] = None,
    **kwargs,
) -> int:
    from dvc.repo.worktree import push_worktree

    if revs or all_branches or all_tags or all_commits:
        raise InvalidArgumentError(
            "Multiple rev push is unsupported for cloud versioned remotes"
        )

    return push_worktree(repo, remote, targets=targets, jobs=jobs, **kwargs)
