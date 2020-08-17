import logging

from dvc.cache import NamedCache
from dvc.exceptions import InvalidArgumentError

from . import locked

logger = logging.getLogger(__name__)


def _do_gc(typ, remote, clist, jobs=None):
    from dvc.remote.base import Remote

    removed = Remote.gc(clist, remote, jobs=jobs)
    if not removed:
        logger.info(f"No unused '{typ}' cache to remove.")


def _raise_error_if_all_disabled(**kwargs):
    if not any(kwargs.values()):
        raise InvalidArgumentError(
            "Either of `-w|--workspace`, `-a|--all-branches`, `-T|--all-tags` "
            "or `--all-commits` needs to be set."
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
    )

    from contextlib import ExitStack

    from dvc.repo import Repo

    if not repos:
        repos = []
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

    # treat caches as remotes for garbage collection
    _do_gc("local", self.cache.local, used, jobs)

    if self.cache.s3:
        _do_gc("s3", self.cache.s3, used, jobs)

    if self.cache.gs:
        _do_gc("gs", self.cache.gs, used, jobs)

    if self.cache.ssh:
        _do_gc("ssh", self.cache.ssh, used, jobs)

    if self.cache.hdfs:
        _do_gc("hdfs", self.cache.hdfs, used, jobs)

    if self.cache.azure:
        _do_gc("azure", self.cache.azure, used, jobs)

    if cloud:
        _do_gc("remote", self.cloud.get_remote(remote, "gc -c"), used, jobs)
