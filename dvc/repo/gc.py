import logging
from typing import TYPE_CHECKING, List, Optional

from dvc.exceptions import InvalidArgumentError

from . import locked

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)


def _validate_args(**kwargs):
    if not any(kwargs.values()):
        raise InvalidArgumentError(
            "Either of `-w|--workspace`, `-a|--all-branches`, `-T|--all-tags` "
            "`--all-experiments`, `--all-commits`, `--date` or `--rev` "
            "needs to be set."
        )
    if kwargs.get("num") and not kwargs.get("rev"):
        raise InvalidArgumentError("`--num` can only be used alongside `--rev`")


@locked
def gc(  # noqa: PLR0913
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
    )

    from contextlib import ExitStack

    from dvc.repo import Repo
    from dvc_data.hashfile.db import get_index
    from dvc_data.hashfile.gc import gc as ogc

    if not repos:
        repos = []
    all_repos = [Repo(path) for path in repos]

    used_obj_ids = set()
    with ExitStack() as stack:
        for repo in all_repos:
            stack.enter_context(repo.lock)

        for repo in [*all_repos, self]:
            for obj_ids in repo.used_objs(
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
            ).values():
                used_obj_ids.update(obj_ids)

    for scheme, odb in self.cache.by_scheme():
        if not odb:
            continue

        removed = ogc(odb, used_obj_ids, jobs=jobs)
        if not removed:
            logger.info("No unused '%s' cache to remove.", scheme)

    if not cloud:
        return

    odb = self.cloud.get_remote_odb(remote, "gc -c")
    removed = ogc(odb, used_obj_ids, jobs=jobs)
    if removed:
        get_index(odb).clear()
    else:
        logger.info("No unused cache to remove from remote.")
