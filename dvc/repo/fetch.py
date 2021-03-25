import logging
import os

import doltcli as dolt

from dvc.config import NoRemoteError
from dvc.exceptions import DownloadError, NoOutputOrStageError

from . import locked

logger = logging.getLogger(__name__)


@locked
def fetch(
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
    revs=None,
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
        revs=revs,
    )

    downloaded = 0
    failed = 0

    try:
        if run_cache:
            self.stage_cache.pull(remote)
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

    remote_url = None
    for t in targets:
        from .stage import StageLoad
        # stages = StageLoad(self).load_file(t+".dvc")
        outs = self.find_outs_by_path(t, strict=False)
        if len(outs) != 1:
            continue
        out = outs[0]
        if not out:
            continue
        if getattr(out.hash_info, "dolt_head", None) is not None:
            if not remote_url:
                remotes = self.config.get("remote", None)
                if not remotes:
                    break
                remote_conf = remotes.get(remote, None)
                if not remote_conf:
                    break
                remote_url = "file://" + remote_conf.get("url")
            if not os.path.exists(t):
                dolt.Dolt.clone(remote_url=remote_url, new_dir=t)
                downloaded += 1
            else:
                db = dolt.Dolt(t)
                db.pull(remote=remote)

    return downloaded


def _fetch_external(self, repo_url, repo_rev, files, jobs):
    from dvc.external_repo import external_repo
    from dvc.objects import save
    from dvc.objects.stage import stage
    from dvc.path_info import PathInfo
    from dvc.scm.base import CloneError

    failed = 0

    results = []

    def cb(result):
        results.append(result)

    odb = self.odb.local
    try:
        with external_repo(
            repo_url, repo_rev, cache_dir=odb.cache_dir
        ) as repo:
            root = PathInfo(repo.root_dir)
            for path in files:
                path_info = root / path
                try:
                    used = repo.used_cache(
                        [os.fspath(path_info)],
                        force=True,
                        jobs=jobs,
                        recursive=True,
                    )
                    cb(repo.cloud.pull(used, jobs))
                except (NoOutputOrStageError, NoRemoteError):
                    pass
                obj = stage(
                    odb,
                    path_info,
                    repo.repo_fs,
                    jobs=jobs,
                    follow_subrepos=False,
                )
                save(
                    odb, obj, jobs=jobs, download_callback=cb,
                )
    except CloneError:
        failed += 1
        logger.exception(
            "failed to fetch data for '{}'".format(", ".join(files))
        )

    return sum(results), failed
