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
    all_commits=False,
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
        all_commits=all_commits,
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
    from dvc.external_repo import external_repo, ExternalRepo

    failed, downloaded = 0, 0
    try:
        with external_repo(repo_url, repo_rev) as repo:
            is_dvc_repo = isinstance(repo, ExternalRepo)
            # gather git-only tracked files if dvc repo
            git_files = [] if is_dvc_repo else files
            if is_dvc_repo:
                repo.cache.local.cache_dir = self.cache.local.cache_dir
                with repo.state:
                    cache = NamedCache()
                    for name in files:
                        try:
                            out = repo.find_out_by_relpath(name)
                        except OutputNotFoundError:
                            # try to add to cache if they are git-tracked files
                            git_files.append(name)
                        else:
                            cache.update(out.get_used_cache())

                        try:
                            downloaded += repo.cloud.pull(cache, jobs=jobs)
                        except DownloadError as exc:
                            failed += exc.amount

            d, f = _git_to_cache(self.cache.local, repo.root_dir, git_files)
            downloaded += d
            failed += f
    except CloneError:
        failed += 1
        logger.exception(
            "failed to fetch data for '{}'".format(", ".join(files))
        )

    return downloaded, failed


def _git_to_cache(cache, repo_root, files):
    """Save files from a git repo directly to the cache."""
    failed = set()
    num_downloads = 0
    repo_root = PathInfo(repo_root)
    for file in files:
        info = cache.save_info(repo_root / file)
        if info.get(cache.PARAM_CHECKSUM) is None:
            failed.add(file)
            continue

        if cache.changed_cache(info[cache.PARAM_CHECKSUM]):
            logger.debug("fetched '%s' from '%s' repo", file, repo_root)
            num_downloads += 1
            cache.save(repo_root / file, info, save_link=False)

    if failed:
        logger.exception(
            "failed to fetch data for {}".format(", ".join(failed))
        )

    return num_downloads, len(failed)
