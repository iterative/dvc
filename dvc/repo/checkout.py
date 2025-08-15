import os
from collections import defaultdict
from typing import TYPE_CHECKING

from dvc.exceptions import (
    CheckoutError,
    CheckoutErrorSuggestGit,
    DvcException,
    NoOutputOrStageError,
)
from dvc.log import logger
from dvc.ui import ui
from dvc.utils import relpath

from . import locked

if TYPE_CHECKING:
    from dvc.repo.index import IndexView
    from dvc_data.index import BaseDataIndex, DataIndexEntry, DataIndexKey
    from dvc_data.index.diff import Change
    from dvc_objects.fs.base import FileSystem

logger = logger.getChild(__name__)


def _fspath_dir(path):
    if not os.path.exists(str(path)):
        return str(path)

    path = relpath(path)
    return os.path.join(path, "") if os.path.isdir(path) else path


def _remove_unused_links(repo):
    used = [out.fspath for out in repo.index.outs if out.protocol == "local"]
    unused = repo.state.get_unused_links(used, repo.fs)
    ret = [_fspath_dir(u) for u in unused]
    repo.state.remove_links(unused, repo.fs)
    return ret


def _build_out_changes(
    index: "IndexView", changes: dict["DataIndexKey", "Change"]
) -> dict["DataIndexKey", tuple[str, dict[str, int]]]:
    from dvc_data.index.checkout import MODIFY

    out_keys: list[DataIndexKey] = []
    for out in index.outs:
        if not out.use_cache:
            continue

        ws, key = out.index_key
        if ws != "repo":
            continue
        out_keys.append(key)

    out_stats: dict[DataIndexKey, dict[str, int]]
    out_stats = defaultdict(lambda: defaultdict(int))

    out_changes: dict[DataIndexKey, tuple[str, dict[str, int]]] = {}
    for key, change in changes.items():
        typ = change.typ
        isdir = change.new and change.new.isdir
        for out_key in out_keys:
            if len(out_key) > len(key) or key[: len(out_key)] != out_key:
                continue

            stats = out_stats[out_key]
            if not isdir:
                stats[typ] += 1

            if key == out_key:
                out_changes[out_key] = typ, stats
            elif out_key not in out_changes:
                typ = MODIFY
                out_changes[out_key] = typ, stats
            break

    return out_changes


def _check_can_delete(
    entries: list["DataIndexEntry"],
    index: "BaseDataIndex",
    path: str,
    fs: "FileSystem",
):
    entry_paths = []
    for entry in entries:
        try:
            cache_fs, cache_path = index.storage_map.get_cache(entry)
        except ValueError:
            continue

        if cache_fs.exists(cache_path):
            continue

        entry_paths.append(fs.join(path, *(entry.key or ())))

    if not entry_paths:
        return

    raise DvcException(
        "Can't remove the following unsaved files without confirmation. "
        "Use `--force` to force.\n" + "\n".join(entry_paths)
    )


@locked
def checkout(  # noqa: C901
    self,
    targets=None,
    with_deps=False,
    force=False,
    relink=False,
    recursive=False,
    allow_missing=False,
    **kwargs,
):
    from dvc.repo.index import build_data_index
    from dvc.stage.exceptions import StageFileBadNameError, StageFileDoesNotExistError
    from dvc_data.index.checkout import ADD, DELETE, MODIFY, apply, compare

    stats = {"modified": 0, "added": 0, "deleted": 0}
    changes: dict[str, list[str]] = {"modified": [], "added": [], "deleted": []}

    if not targets:
        targets = [None]
        changes["deleted"] = _remove_unused_links(self)
        stats["deleted"] = len(changes["deleted"])

    if isinstance(targets, str):
        targets = [targets]

    def onerror(target, exc):
        if target and isinstance(
            exc,
            (StageFileDoesNotExistError, StageFileBadNameError, NoOutputOrStageError),
        ):
            raise CheckoutErrorSuggestGit(target) from exc
        raise  # noqa: PLE0704

    from .index import index_from_targets

    view = index_from_targets(
        self, targets=targets, recursive=recursive, with_deps=with_deps, onerror=onerror
    )

    with ui.progress(unit="entry", desc="Building workspace index", leave=True) as pb:
        old = build_data_index(
            view, self.root_dir, self.fs, compute_hash=True, callback=pb.as_callback()
        )

    new = view.data["repo"]

    with ui.progress(desc="Comparing indexes", unit="entry", leave=True) as pb:
        diff = compare(old, new, relink=relink, delete=True, callback=pb.as_callback())

    if not force:
        _check_can_delete(diff.files_delete, new, self.root_dir, self.fs)

    failed = set()
    out_paths = [out.fs_path for out in view.outs if out.use_cache and out.is_in_repo]

    def checkout_onerror(src_path, dest_path, _exc):
        logger.debug(
            "failed to create '%s' from '%s'",
            dest_path,
            src_path,
            exc_info=True,  # noqa: LOG014
        )

        for out_path in out_paths:
            if self.fs.isin_or_eq(dest_path, out_path):
                failed.add(out_path)

    with ui.progress(unit="file", desc="Applying changes", leave=True) as pb:
        apply(
            diff,
            self.root_dir,
            self.fs,
            callback=pb.as_callback(),
            update_meta=False,
            onerror=checkout_onerror,
            state=self.state,
            **kwargs,
        )

    out_changes = _build_out_changes(view, diff.changes)

    typ_map = {ADD: "added", DELETE: "deleted", MODIFY: "modified"}
    for key, (typ, _stats) in out_changes.items():
        out_path = self.fs.join(self.root_dir, *key)

        if out_path in failed:
            self.fs.remove(out_path, recursive=True)
            continue

        self.state.save_link(out_path, self.fs)
        for t, count in _stats.items():
            stats_typ = typ_map[t]
            stats[stats_typ] += count

        changes[typ_map[typ]].append(_fspath_dir(out_path))

    for changelist in changes.values():
        # group directories first, then files. But keep them alphabetically sorted
        changelist.sort(key=lambda p: (not p.endswith(os.sep), p))

    result = changes | {"stats": stats}
    if failed and not allow_missing:
        result["failed"] = [relpath(out_path) for out_path in failed]
        raise CheckoutError([relpath(out_path) for out_path in failed], result)
    return result
