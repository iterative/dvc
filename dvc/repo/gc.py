import logging
from typing import TYPE_CHECKING, List, Optional, Set, Tuple

from dvc.exceptions import InvalidArgumentError

from . import locked

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc_data.hashfile.hash_info import HashInfo
    from dvc_objects.db import ObjectDB

logger = logging.getLogger(__name__)


def _validate_args(**kwargs):
    not_in_remote = kwargs.pop("not_in_remote", None)
    cloud = kwargs.pop("cloud", None)
    remote = kwargs.pop("remote", None)
    if remote and not (cloud or not_in_remote):
        raise InvalidArgumentError("`--remote` requires `--cloud` or `--not-in-remote`")
    if not_in_remote and cloud:
        raise InvalidArgumentError(
            "`--not-in-remote` and `--cloud` are mutually exclusive"
        )
    if not any(kwargs.values()):
        raise InvalidArgumentError(
            "Either of `-w|--workspace`, `-a|--all-branches`, `-T|--all-tags` "
            "`--all-experiments`, `--all-commits`, `--date` or `--rev` "
            "needs to be set."
        )
    if kwargs.get("num") and not kwargs.get("rev"):
        raise InvalidArgumentError("`--num` can only be used alongside `--rev`")


def _used_obj_ids_not_in_remote(repo, default_remote, jobs, remote_odb_to_obj_ids):
    used_obj_ids = set()
    remote_oids = set()
    for remote_odb, obj_ids in remote_odb_to_obj_ids:
        if remote_odb is None:
            remote_odb = repo.cloud.get_remote_odb(default_remote, "gc --not-in-remote")

        remote_oids.update(
            remote_odb.list_oids_exists({x.value for x in obj_ids}, jobs=jobs)
        )
        used_obj_ids.update(obj_ids)
    return {obj for obj in used_obj_ids if obj.value not in remote_oids}


@locked
def gc(  # noqa: PLR0912, PLR0913, C901
    self: "Repo",
    all_branches: bool = False,
    cloud: bool = False,
    remote: Optional[str] = None,
    with_deps: bool = False,
    all_tags: bool = False,
    all_commits: bool = False,
    all_experiments: bool = False,
    force: bool = False,
    jobs: Optional[int] = None,
    repos: Optional[List[str]] = None,
    workspace: bool = False,
    commit_date: Optional[str] = None,
    rev: Optional[str] = None,
    num: Optional[int] = None,
    not_in_remote: bool = False,
):
    # require `workspace` to be true to come into effect.
    # assume `workspace` to be enabled if any of `all_tags`, `all_commits`,
    # `all_experiments` or `all_branches` are enabled.
    _validate_args(
        workspace=workspace,
        all_tags=all_tags,
        all_commits=all_commits,
        all_branches=all_branches,
        all_experiments=all_experiments,
        commit_date=commit_date,
        rev=rev,
        num=num,
        cloud=cloud,
        not_in_remote=not_in_remote,
    )

    from contextlib import ExitStack

    from dvc.repo import Repo
    from dvc_data.hashfile.db import get_index
    from dvc_data.hashfile.gc import gc as ogc

    if not repos:
        repos = []
    all_repos = [Repo(path) for path in repos]

    odb_to_obj_ids: List[Tuple[Optional["ObjectDB"], Set["HashInfo"]]] = []

    with ExitStack() as stack:
        for repo in all_repos:
            stack.enter_context(repo.lock)

        for repo in [*all_repos, self]:
            for odb, obj_ids in repo.used_objs(
                all_branches=all_branches,
                with_deps=with_deps,
                all_tags=all_tags,
                all_commits=all_commits,
                all_experiments=all_experiments,
                commit_date=commit_date,
                remote=remote,
                force=force,
                jobs=jobs,
                revs=[rev] if rev else None,
                num=num or 1,
            ).items():
                odb_to_obj_ids.append((odb, obj_ids))

    if not odb_to_obj_ids:
        odb_to_obj_ids = [(None, set())]

    if not_in_remote:
        used_obj_ids = _used_obj_ids_not_in_remote(self, remote, jobs, odb_to_obj_ids)
    else:
        used_obj_ids = set()
        for _, obj_ids in odb_to_obj_ids:
            used_obj_ids.update(obj_ids)

    for scheme, odb in self.cache.by_scheme():
        if not odb:
            continue
        removed = ogc(odb, used_obj_ids, jobs=jobs)
        if not removed:
            logger.info("No unused '%s' cache to remove.", scheme)

    if not cloud:
        return

    for remote_odb, obj_ids in odb_to_obj_ids:
        if remote_odb is None:
            remote_odb = repo.cloud.get_remote_odb(remote, "gc -c")
        removed = ogc(remote_odb, obj_ids, jobs=jobs)
        if removed:
            get_index(remote_odb).clear()
        else:
            logger.info("No unused cache to remove from remote.")
