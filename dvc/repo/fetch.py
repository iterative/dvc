import logging

from dvc.config import NoRemoteError
from dvc.exceptions import DownloadError

from ..scheme import Schemes
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
    from dvc.objects.db.git import GitObjectDB

    if isinstance(targets, str):
        targets = [targets]

    used = self.used_objs(
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
    except DownloadError as exc:
        failed += exc.amount

    external = set()
    for odb, objs in used.items():
        if odb is None:
            # objs contains naive objects to be pulled from specified remote
            d, f = _fetch_naive_objs(
                self,
                objs,
                jobs=jobs,
                remote=remote,
                show_checksums=show_checksums,
            )
            downloaded += d
            failed += f
        elif isinstance(odb, GitObjectDB):
            # objs contains staged import objects which should be saved
            # last (after all other objects have been pulled)
            external.update(objs)
        else:
            d, f = fetch_from_odb(
                self,
                odb,
                objs,
                jobs=jobs,
                show_checksums=show_checksums,
            )
            downloaded += d
            failed += f

    if external:
        d, f = _fetch_external(self, external, jobs=jobs)
        downloaded += d
        failed += f

    if failed:
        raise DownloadError(failed)

    return downloaded


def _fetch_naive_objs(repo, objs, **kwargs):
    # objs contains naive objects to be pulled from specified remote
    downloaded = 0
    failed = 0
    try:
        downloaded += repo.cloud.pull(objs, **kwargs)
    except NoRemoteError:
        if any(obj.fs.scheme == Schemes.LOCAL for obj in objs):
            raise
    except DownloadError as exc:
        failed += exc.amount
    return downloaded, failed


def fetch_from_odb(repo, odb, objs, **kwargs):
    from dvc.remote.base import Remote

    downloaded = 0
    failed = 0
    remote = Remote.from_odb(odb)
    try:
        downloaded += remote.pull(
            repo.odb.local,
            objs,
            **kwargs,
        )
    except DownloadError as exc:
        failed += exc.amount
    return downloaded, failed


def _fetch_external(repo, objs, **kwargs):
    from dvc.objects import save
    from dvc.objects.errors import ObjectError

    results = []
    failed = 0

    def callback(result):
        results.append(result)

    for obj in objs:
        try:
            save(repo.odb.local, obj, download_callback=callback, **kwargs)
        except ObjectError:
            failed += 1

    return sum(results), failed
