from typing import TYPE_CHECKING

from dvc.exceptions import DownloadError
from dvc.log import logger
from dvc.stage.cache import RunCacheNotSupported
from dvc.ui import ui
from dvc_data.index import DataIndex, FileStorage

from . import locked

if TYPE_CHECKING:
    from dvc.output import Output
    from dvc.repo import Repo
    from dvc.stage import Stage

logger = logger.getChild(__name__)


def _make_index_onerror(onerror, rev):
    def _onerror(entry, exc):
        if onerror:
            return onerror(rev, entry, exc)

    return _onerror


def _collect_indexes(  # noqa: PLR0913
    repo: "Repo",
    targets=None,
    remote=None,
    all_branches=False,
    with_deps=False,
    all_tags=False,
    recursive=False,
    all_commits=False,
    revs=None,
    workspace=True,
    max_size=None,
    types=None,
    config=None,
    onerror=None,
    push=False,
):
    from .index import index_from_targets

    indexes = {}
    collection_exc = None

    config = config or {}
    if remote:
        core = config.get("core") or {}
        core["remote"] = remote
        config["core"] = core

    def stage_filter(stage: "Stage") -> bool:
        return not (push and stage.is_repo_import)

    def outs_filter(out: "Output") -> bool:
        if push and not out.can_push:
            return False
        return not (remote and out.remote and remote != out.remote)

    for rev in repo.brancher(
        revs=revs,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
        workspace=workspace,
    ):
        try:
            repo.config.merge(config)

            idx = index_from_targets(
                repo,
                targets,
                with_deps=with_deps,
                recursive=recursive,
                max_size=max_size,
                types=types,
                stage_filter=stage_filter,
                outs_filter=outs_filter,
            )

            idx.data["repo"].onerror = _make_index_onerror(onerror, rev)

            indexes[rev or "workspace"] = idx
        except Exception as exc:  # noqa: BLE001
            if onerror:
                onerror(rev, None, exc)
            collection_exc = exc
            logger.warning("failed to collect '%s', skipping", rev or "workspace")

    if not indexes and collection_exc:
        raise collection_exc

    return indexes


@locked
def fetch(  # noqa: PLR0913
    self: "Repo",
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
    workspace=True,
    max_size=None,
    types=None,
    config=None,
    onerror=None,
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

    from dvc_data.index.fetch import collect
    from dvc_data.index.fetch import fetch as ifetch

    if isinstance(targets, str):
        targets = [targets]

    failed_count = 0
    transferred_count = 0

    try:
        if run_cache:
            self.stage_cache.pull(remote)
    except RunCacheNotSupported as e:
        logger.debug("failed to pull run cache: %s", e)
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
        workspace=workspace,
        max_size=max_size,
        types=types,
        config=config,
        onerror=onerror,
    )

    cache_key = (
        "fetch",
        tokenize(sorted(idx.data_tree.hash_info.value for idx in indexes.values())),
    )

    with ui.progress(desc="Collecting", unit="entry", leave=True) as pb:
        data = collect(
            [idx.data["repo"] for idx in indexes.values()],
            "remote",
            cache_index=self.data_index,
            cache_key=cache_key,
            callback=pb.as_callback(),
        )
    data, unversioned_count = _log_unversioned(data)
    failed_count += unversioned_count

    with ui.progress(
        desc="Fetching",
        bar_format="{desc}",
        leave=True,
    ) as pb:
        try:
            fetch_transferred, fetch_failed = ifetch(
                data,
                jobs=jobs,
                callback=pb.as_callback(),
            )
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


def _log_unversioned(data: list["DataIndex"]) -> tuple[list["DataIndex"], int]:
    ret: list[DataIndex] = []
    unversioned: list[str] = []
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
                unversioned.append(fs.unstrip_protocol(fs.join(remote.path, *key)))
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
