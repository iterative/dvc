from contextlib import suppress

from dvc.exceptions import InvalidArgumentError, UploadError
from dvc.log import logger
from dvc.stage.cache import RunCacheNotSupported
from dvc.ui import ui

from . import locked

logger = logger.getChild(__name__)


def _rebuild(idx, path, fs, cb):
    from dvc_data.index import DataIndex, DataIndexEntry, Meta

    new = DataIndex()
    items = list(idx.items())

    cb.set_size(len(items))
    for key, entry in items:
        if entry.meta and entry.meta.isdir:
            meta = Meta(isdir=True)
        else:
            try:
                meta = Meta.from_info(fs.info(fs.join(path, *key)), fs.protocol)
            except FileNotFoundError:
                meta = None

        if meta:
            new.add(DataIndexEntry(key=key, meta=meta))

        cb.relative_update(1)

    return new


def _update_meta(index, **kwargs):
    from dvc.repo.worktree import _merge_push_meta, worktree_view_by_remotes

    stages = set()
    for remote_name, idx in worktree_view_by_remotes(index, push=True, **kwargs):
        remote = index.repo.cloud.get_remote(remote_name)

        if not remote.fs.version_aware:
            continue

        with ui.progress(
            desc=f"Collecting {remote.path} on {remote.fs.protocol}",
            unit="entry",
            leave=True,
        ) as pb:
            cb = pb.as_callback()
            new = _rebuild(idx.data["repo"], remote.path, remote.fs, cb)

        for out in idx.outs:
            _merge_push_meta(out, new, remote.name)
            stages.add(out.stage)

    for stage in stages:
        stage.dump(with_files=True, update_pipeline=False)


@locked
def push(  # noqa: PLR0913
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
    workspace=True,
    glob=False,
):
    from fsspec.utils import tokenize

    from dvc.config import NoRemoteError
    from dvc.utils import glob_targets
    from dvc_data.index.fetch import collect
    from dvc_data.index.push import push as ipush

    from .fetch import _collect_indexes

    failed_count = 0
    transferred_count = 0

    with suppress(NoRemoteError):
        _remote = self.cloud.get_remote(name=remote)
        if (
            _remote
            and (_remote.worktree or _remote.fs.version_aware)
            and (revs or all_branches or all_tags or all_commits)
        ):
            raise InvalidArgumentError(
                "Multiple rev push is unsupported for cloud versioned remotes"
            )

    try:
        used_run_cache = self.stage_cache.push(remote) if run_cache else []
        transferred_count += len(used_run_cache)
    except RunCacheNotSupported as e:
        logger.debug("failed to push run cache: %s", e)

    if isinstance(targets, str):
        targets = [targets]

    indexes = _collect_indexes(
        self,
        targets=glob_targets(targets, glob=glob),
        remote=remote,
        all_branches=all_branches,
        with_deps=with_deps,
        all_tags=all_tags,
        recursive=recursive,
        all_commits=all_commits,
        revs=revs,
        workspace=workspace,
        push=True,
    )

    cache_key = (
        "push",
        tokenize(sorted(idx.data_tree.hash_info.value for idx in indexes.values())),
    )

    with ui.progress(desc="Collecting", unit="entry", leave=True) as pb:
        data = collect(
            [idx.data["repo"] for idx in indexes.values()],
            "remote",
            cache_index=self.data_index,
            cache_key=cache_key,
            callback=pb.as_callback(),
            push=True,
        )

    push_transferred, push_failed = 0, 0
    try:
        with ui.progress(
            desc="Pushing",
            bar_format="{desc}",
            leave=True,
        ) as pb:
            push_transferred, push_failed = ipush(
                data,
                jobs=jobs,
                callback=pb.as_callback(),
            )
    finally:
        ws_idx = indexes.get("workspace")
        if ws_idx is not None:
            _update_meta(
                self.index,
                targets=glob_targets(targets, glob=glob),
                remote=remote,
                with_deps=with_deps,
                recursive=recursive,
            )

        for fs_index in data:
            fs_index.close()

        if push_transferred:
            # NOTE: dropping cached index to force reloading from newly saved
            # metadata from version-aware remotes
            self.drop_data_index()

    transferred_count += push_transferred
    failed_count += push_failed
    if failed_count:
        raise UploadError(failed_count)

    return transferred_count
