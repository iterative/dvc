import logging

from dvc.exceptions import InvalidArgumentError

from . import locked

logger = logging.getLogger(__name__)


def _raise_error_if_all_disabled(**kwargs):
    if not any(kwargs.values()):
        raise InvalidArgumentError(
            "Either of `-w|--workspace`, `-a|--all-branches`, `-T|--all-tags` "
            "`--all-experiments` or `--all-commits` needs to be set."
        )


@locked
def gc(
    self,
    all_branches=False,
    cloud=False,
    remote=None,
    with_deps=False,
    all_tags=False,
    all_commits=False,
    all_experiments=False,
    force=False,
    jobs=None,
    repos=None,
    workspace=False,
):

    # require `workspace` to be true to come into effect.
    # assume `workspace` to be enabled if any of `all_tags`, `all_commits`,
    # `all_experiments` or `all_branches` are enabled.
    _raise_error_if_all_disabled(
        workspace=workspace,
        all_tags=all_tags,
        all_commits=all_commits,
        all_branches=all_branches,
        all_experiments=all_experiments,
    )

    from contextlib import ExitStack

    from dvc.data.db import get_index
    from dvc.data.gc import gc as ogc
    from dvc.repo import Repo

    if not repos:
        repos = []
    all_repos = [Repo(path) for path in repos]

    used_obj_ids = set()
    with ExitStack() as stack:
        for repo in all_repos:
            stack.enter_context(repo.lock)

        for repo in all_repos + [self]:
            for obj_ids in repo.used_objs(
                all_branches=all_branches,
                with_deps=with_deps,
                all_tags=all_tags,
                all_commits=all_commits,
                all_experiments=all_experiments,
                remote=remote,
                force=force,
                jobs=jobs,
            ).values():
                used_obj_ids.update(obj_ids)

    for scheme, odb in self.odb.by_scheme():
        if not odb:
            continue

        removed = ogc(odb, used_obj_ids, jobs=jobs)
        if not removed:
            logger.info(f"No unused '{scheme}' cache to remove.")

    if not cloud:
        return

    odb = self.cloud.get_remote_odb(remote, "gc -c")
    removed = ogc(odb, used_obj_ids, jobs=jobs)
    if removed:
        get_index(odb).clear()
    else:
        logger.info("No unused cache to remove from remote.")
