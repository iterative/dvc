import logging

from dvc.exceptions import DownloadError, FileTransferError
from dvc.scheme import Schemes

from . import locked

logger = logging.getLogger(__name__)


@locked
def fetch(
    self,
    targets=None,
    jobs=None,
    remote=None,
    all_branches=False,
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

    for odb, obj_ids in sorted(
        used.items(),
        key=lambda item: item[0] is not None
        and item[0].fs.scheme == Schemes.MEMORY,
    ):
        d, f = _fetch(
            self,
            obj_ids,
            jobs=jobs,
            remote=remote,
            odb=odb,
        )
        downloaded += d
        failed += f

    if failed:
        raise DownloadError(failed)

    return downloaded


def _fetch(repo, obj_ids, **kwargs):
    downloaded = 0
    failed = 0
    try:
        downloaded += repo.cloud.pull(obj_ids, **kwargs)
    except FileTransferError as exc:
        failed += exc.amount
    return downloaded, failed
