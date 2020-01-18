import logging

from dvc.cache import NamedCache
from dvc.config import NoRemoteError
from dvc.exceptions import DownloadError
from dvc.exceptions import OutputNotFoundError
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
    used = self.used_cache(
        targets,
        all_branches=all_branches,
        all_tags=all_tags,
        with_deps=with_deps,
        force=True,
        remote=remote,
        jobs=jobs,
        recursive=recursive,
    )

    downloaded = 0
    failed = 0

    try:
        downloaded += self.cloud.pull(
            used, jobs, remote=remote, show_checksums=show_checksums
        )
    except NoRemoteError:
        if not used.external and used["local"]:
            raise
    except DownloadError as exc:
        failed += exc.amount

    for (repo_url, repo_rev), files in used.external.items():
        d, f = _fetch_external(self, repo_url, repo_rev, files)
        downloaded += d
        failed += f

    if failed:
        raise DownloadError(failed)

    return downloaded


def _fetch_external(self, repo_url, repo_rev, files):
    from dvc.external_repo import external_repo

    failed = 0
    try:
        with external_repo(repo_url, repo_rev) as repo:
            repo.cache.local.cache_dir = self.cache.local.cache_dir

            with repo.state:
                cache = NamedCache()
                for name in files:
                    try:
                        out = repo.find_out_by_relpath(name)
                    except OutputNotFoundError:
                        failed += 1
                        logger.exception(
                            "failed to fetch data for '{}'".format(name)
                        )
                        continue
                    else:
                        cache.update(out.get_used_cache())

                try:
                    return repo.cloud.pull(cache), failed
                except DownloadError as exc:
                    failed += exc.amount
    except CloneError:
        failed += 1
        logger.exception(
            "failed to fetch data for '{}'".format(", ".join(files))
        )

    return 0, failed
