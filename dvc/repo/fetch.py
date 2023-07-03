import logging
from typing import Dict, List, Tuple

from dvc.exceptions import DownloadError
from dvc_data.index import DataIndex, FileStorage

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
    max_size=None,
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
    from fsspec.utils import tokenize

    from dvc.fs.callbacks import Callback
    from dvc_data.index.fetch import collect
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

    indexes = []
    index_keys = set()
    for _ in self.brancher(
        revs=revs,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
    ):
        saved_remote = self.config["core"].get("remote")
        try:
            if remote:
                self.config["core"]["remote"] = remote

            idx = self.index.targets_view(
                targets,
                with_deps=with_deps,
                recursive=recursive,
                max_size=max_size,
            )
            index_keys.add(idx.data_tree.hash_info.value)
            indexes.append(idx.data["repo"])
        finally:
            if remote:
                self.config["core"]["remote"] = saved_remote

    cache_key = ("fetch", tokenize(sorted(index_keys)))

    with Callback.as_tqdm_callback(
        desc="Collecting",
        unit="entry",
    ) as cb:
        data = collect(
            indexes, cache_index=self.data_index, cache_key=cache_key, callback=cb
        )
    failed_count += _log_unversioned(data)

    with Callback.as_tqdm_callback(
        desc="Fetching",
        unit="file",
    ) as cb:
        try:
            fetch_transferred, fetch_failed = ifetch(
                data,
                jobs=jobs,
                callback=cb,
            )  # pylint: disable=assignment-from-no-return
        finally:
            for fs_index in data.values():
                fs_index.close()

    if fetch_transferred:
        # NOTE: dropping cached index to force reloading from newly saved cache
        self.drop_data_index()

    transferred_count += fetch_transferred
    failed_count += fetch_failed
    if failed_count:
        raise DownloadError(failed_count)

    return transferred_count


def _log_unversioned(data: Dict[Tuple[str, str], "DataIndex"]) -> int:
    unversioned: List[str] = []
    for by_fs, fs_index in data.items():
        remote = fs_index.storage_map[()].remote
        if not isinstance(remote, FileStorage) or not remote.fs.version_aware:
            continue

        fs = remote.fs
        index = DataIndex()
        index.storage_map = fs_index.storage_map
        for key, entry in fs_index.iteritems():
            if entry.meta and not entry.meta.isdir and entry.meta.version_id is None:
                unversioned.append(fs.unstrip_protocol(fs.path.join(remote.path, *key)))
            else:
                index[key] = entry
        fs_index.close()
        data[by_fs] = index
    if unversioned:
        logger.warning(
            (
                "Some files are missing cloud version information and will not be "
                "fetched from the remote:\n%s"
            ),
            "\n".join(unversioned),
        )
    return len(unversioned)
