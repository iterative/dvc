import logging

from . import locked
from dvc.cache import NamedCache
from dvc.exceptions import InvalidArgumentError


logger = logging.getLogger(__name__)


def _do_gc(typ, func, clist):
    removed = func(clist)
    if not removed:
        logger.info("No unused '{}' cache to remove.".format(typ))


def _raise_error_if_all_disabled(**kwargs):
    if not any(kwargs.values()):
        raise InvalidArgumentError(
            "Invalid Arguments. Either of ({}) needs to be enabled.".format(
                ", ".join(kwargs.keys())
            )
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
    force=False,
    jobs=None,
    repos=None,
    workspace=False,
):

    # require `workspace` to be true to come into effect.
    # assume `workspace` to be enabled if any of `all_tags`, `all_commits`,
    # or `all_branches` are enabled.
    _raise_error_if_all_disabled(
        workspace=workspace,
        all_tags=all_tags,
        all_commits=all_commits,
        all_branches=all_branches,
        cloud=cloud,
    )

    from contextlib import ExitStack
    from dvc.repo import Repo

    all_repos = []

    if repos:
        all_repos = [Repo(path) for path in repos]

    with ExitStack() as stack:
        for repo in all_repos:
            stack.enter_context(repo.lock)
            stack.enter_context(repo.state)

        used = NamedCache()
        for repo in all_repos + [self]:
            used.update(
                repo.used_cache(
                    all_branches=all_branches,
                    with_deps=with_deps,
                    all_tags=all_tags,
                    all_commits=all_commits,
                    remote=remote,
                    force=force,
                    jobs=jobs,
                )
            )

    _do_gc("local", self.cache.local.gc, used)

    if self.cache.s3:
        _do_gc("s3", self.cache.s3.gc, used)

    if self.cache.gs:
        _do_gc("gs", self.cache.gs.gc, used)

    if self.cache.ssh:
        _do_gc("ssh", self.cache.ssh.gc, used)

    if self.cache.hdfs:
        _do_gc("hdfs", self.cache.hdfs.gc, used)

    if self.cache.azure:
        _do_gc("azure", self.cache.azure.gc, used)

    if cloud:
        _do_gc("remote", self.cloud.get_remote(remote, "gc -c").gc, used)
