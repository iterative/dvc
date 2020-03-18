import logging

from dvc.cache import NamedCache
from dvc.config import NoRemoteError
from dvc.exceptions import DownloadError, OutputNotFoundError
from dvc.scm.base import CloneError
from dvc.path_info import PathInfo


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
        d, f = _fetch_external(self, repo_url, repo_rev, files, jobs)
        downloaded += d
        failed += f

    if failed:
        raise DownloadError(failed)

    return downloaded


def _fetch_external(self, repo_url, repo_rev, files, jobs):
    from dvc.external_repo import external_repo

    failed = 0
    try:
        with external_repo(repo_url, repo_rev) as repo:
            if not hasattr(repo, "cache"):
                return _fetch_external_git(
                    self.cache.local, repo.root_dir, files
                )

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
                    return repo.cloud.pull(cache, jobs=jobs), failed
                except DownloadError as exc:
                    failed += exc.amount
    except CloneError:
        failed += 1
        logger.exception(
            "failed to fetch data for '{}'".format(", ".join(files))
        )

    return 0, failed


def _fetch_external_git(cache, root_dir, files):
    failed, downloaded = 0, 0
    root_dir = PathInfo(root_dir)
    for file in files:
        info = cache.save_info(root_dir / file)
        if info.get(cache.PARAM_CHECKSUM) is None:
            failed += 1
            continue

        if cache.changed_cache(info[cache.PARAM_CHECKSUM]):
            downloaded += 1
            cache.save(root_dir / file, info)

    return downloaded, failed
