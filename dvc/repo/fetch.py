import logging

from dvc.config import NoRemoteError
from dvc.exceptions import DownloadError
from dvc.scm.base import CloneError

logger = logging.getLogger(__name__)


def _fetch(
    self,
    targets=None,
    jobs=None,
    remote=None,
    all_branches=False,
    show_checksums=False,
    with_deps=False,
    all_tags=False,
    recursive=False,
    all_commits=False,
    run_cache=False,
):
    """Download data items from a cloud and imported repositories

    Returns:
        int: number of successfully downloaded files

    Raises:
        DownloadError: thrown when there are failed downloads, either
            during `cloud.pull` or trying to fetch imported files

        config.NoRemoteError: thrown when downloading only local files and no
            remote is configured
    """

    used_run_cache = self.stage_cache.pull(remote) if run_cache else []

    if isinstance(targets, str):
        targets = [targets]

    used = self.used_cache(
        targets,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
        with_deps=with_deps,
        force=True,
        remote=remote,
        jobs=jobs,
        recursive=recursive,
        used_run_cache=used_run_cache,
    )

    downloaded = 0
    failed = 0

    try:
        downloaded += self.cloud.pull(
            used, jobs, remote=remote, show_checksums=show_checksums,
        )
    except NoRemoteError:
        if not used.external and used["local"]:
            raise
    except DownloadError as exc:
        failed += exc.amount

    for (repo_url, repo_rev), files in used.external.items():
        d, f = _fetch_external(self, repo_url, repo_rev, files, jobs)
        downloaded += d
        failed += f

    if failed:
        raise DownloadError(failed)

    return downloaded + len(used_run_cache)


def _fetch_external(self, repo_url, repo_rev, files, jobs):
    from dvc.external_repo import external_repo

    failed, downloaded = 0, 0
    try:
        with external_repo(repo_url, repo_rev) as repo:
            with repo.use_cache(self.cache.local):
                d, f, _ = repo.fetch_external(files, jobs=jobs)
                downloaded += d
                failed += f
    except CloneError:
        failed += 1
        logger.exception(
            "failed to fetch data for '{}'".format(", ".join(files))
        )

    return downloaded, failed
