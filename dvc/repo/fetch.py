import logging
from typing import List, Tuple

from dvc.exceptions import DownloadError
from dvc_data.index import DataIndex, FileStorage

from . import locked

logger = logging.getLogger(__name__)


def _collect_indexes(  # noqa: PLR0913
    repo,
    targets=None,
    remote=None,
    all_branches=False,
    with_deps=False,
    all_tags=False,
    recursive=False,
    all_commits=False,
    revs=None,
    max_size=None,
    types=None,
    config=None,
):
    indexes = {}
    collection_exc = None

    config = config or {}
    if remote:
        core = config.get("core") or {}
        core["remote"] = remote
        config["core"] = core

    for rev in repo.brancher(
        revs=revs,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
    ):
        try:
            repo.config.update(config)

            idx = repo.index.targets_view(
                targets,
                with_deps=with_deps,
                recursive=recursive,
                max_size=max_size,
                types=types,
            )
            indexes[idx.data_tree.hash_info.value] = idx.data["repo"]
        except Exception as exc:  # pylint: disable=broad-except
            collection_exc = exc
            logger.exception("failed to collect '%s'", rev or "workspace")

    if not indexes and collection_exc:
        raise collection_exc

    return indexes


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
    types=None,
    config=None,
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

    indexes = _collect_indexes(
        self,
        targets=targets,
        remote=remote,
        all_branches=all_branches,
        with_deps=with_deps,
        all_tags=all_tags,
        recursive=recursive,
        all_commits=all_commits,
        revs=revs,
        max_size=max_size,
        types=types,
        config=config,
    )

    cache_key = ("fetch", tokenize(sorted(indexes.keys())))

    with Callback.as_tqdm_callback(
        desc="Collecting",
        unit="entry",
    ) as cb:
        data = collect(
            indexes.values(),
            "remote",
            cache_index=self.data_index,
            cache_key=cache_key,
            callback=cb,
        )
    data, unversioned_count = _log_unversioned(data)
    failed_count += unversioned_count

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
            for fs_index in data:
                fs_index.close()

    if fetch_transferred:
        # NOTE: dropping cached index to force reloading from newly saved cache
        self.drop_data_index()

    transferred_count += fetch_transferred
    failed_count += fetch_failed
    if failed_count:
        raise DownloadError(failed_count)

    return transferred_count


def _log_unversioned(data: List["DataIndex"]) -> Tuple[List["DataIndex"], int]:
    ret: List["DataIndex"] = []
    unversioned: List[str] = []
    for fs_index in data:
        remote = fs_index.storage_map[()].remote
        if not isinstance(remote, FileStorage) or not remote.fs.version_aware:
            ret.append(fs_index)
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
        ret.append(index)

    if unversioned:
        logger.warning(
            (
                "Some files are missing cloud version information and will not be "
                "fetched from the remote:\n%s"
            ),
            "\n".join(unversioned),
        )
    return ret, len(unversioned)
