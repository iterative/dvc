import logging

from funcy import lsplit

from dvc.config import NoRemoteError
from dvc.exceptions import DownloadError
from dvc.objects.external import ExternalRepoFile

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

    if isinstance(targets, str):
        targets = [targets]

    objs = self.used_objs(
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
    used_external, used_objs = lsplit(
        lambda x: isinstance(x, ExternalRepoFile), objs
    )

    downloaded = 0
    failed = 0

    try:
        if run_cache:
            self.stage_cache.pull(remote)
        downloaded += self.cloud.pull(
            used_objs,
            jobs,
            remote=remote,
            show_checksums=show_checksums,
        )
    except NoRemoteError:
        if not used_external and any(
            obj.fs.scheme == Schemes.LOCAL for obj in used_objs
        ):
            raise
    except DownloadError as exc:
        failed += exc.amount

    if used_external:
        d, f = _fetch_external(self, used_external, jobs)
        downloaded += d
        failed += f

    if failed:
        raise DownloadError(failed)

    return downloaded


def _fetch_external(self, external_objs, jobs):
    from dvc.objects import save
    from dvc.objects.errors import ObjectError

    failed = 0

    results = []

    def cb(result):
        results.append(result)

    odb = self.odb.local
    for obj in external_objs:
        try:
            save(odb, obj, jobs=jobs, download_callback=cb)
        except (ObjectError, OSError):
            failed += 1
            logger.exception(
                "failed to fetch data for '{}'".format(
                    ", ".join(obj.path_info)
                )
            )

    return sum(results), failed
