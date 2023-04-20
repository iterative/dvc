import logging

from dvc.exceptions import DownloadError

from . import locked

logger = logging.getLogger(__name__)


@locked
def fetch(  # noqa: C901, PLR0913
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
) -> int:
    """Download data items from a cloud and imported repositories

    Returns:
        int: number of successfully downloaded files

    Raises:
        DownloadError: thrown when there are failed downloads, either
            during `cloud.pull` or trying to fetch imported files

        config.NoRemoteError: thrown when downloading only local files and no
            remote is configured
    """
    from dvc_data.index.fetch import fetch as ifetch

    if isinstance(targets, str):
        targets = [targets]

    failed_count = 0
    transferred_count = 0

    try:
        if run_cache:
            self.stage_cache.pull(remote)
    except DownloadError as exc:
        failed_count += exc.amount

    def _indexes():
        for _ in self.brancher(
            revs=revs,
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
        ):
            yield self.index.targets_view(
                targets,
                with_deps=with_deps,
                recursive=recursive,
            ).data["repo"]

    saved_remote = self.config["core"].get("remote")
    try:
        if remote:
            self.config["core"]["remote"] = remote

        fetch_transferred, fetch_failed = ifetch(
            _indexes(), jobs=jobs
        )  # pylint: disable=assignment-from-no-return
    finally:
        if remote:
            self.config["core"]["remote"] = saved_remote

    if fetch_transferred:
        # NOTE: dropping cached index to force reloading from newly saved cache
        self.drop_data_index()

    transferred_count += fetch_transferred
    failed_count += fetch_failed
    if failed_count:
        raise DownloadError(failed_count)

    return transferred_count
