import os
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
    from dvc_data.index import BaseDataIndex, DataIndexEntry
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


def _build_out_changes(index, changes):
    from dvc_data.index.checkout import MODIFY

    out_keys = []
    for out in index.outs:
        if not out.use_cache:
            continue

        ws, key = out.index_key
        if ws != "repo":
            continue

        out_keys.append(key)

    out_changes = {}
    for key, change in changes.items():
        for out_key in out_keys:
            if len(out_key) > len(key) or key[: len(out_key)] != out_key:
                continue

            if key == out_key:
                out_changes[out_key] = change.typ
            elif not out_changes.get(out_key):
                out_changes[out_key] = MODIFY
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

    stats: dict[str, list[str]] = {
        "added": [],
        "deleted": [],
        "modified": [],
    }
    if not targets:
        targets = [None]
        stats["deleted"] = _remove_unused_links(self)

    if isinstance(targets, str):
        targets = [targets]

    def onerror(target, exc):
        if target and isinstance(
            exc,
            (StageFileDoesNotExistError, StageFileBadNameError, NoOutputOrStageError),
        ):
            raise CheckoutErrorSuggestGit(target) from exc
        raise  # noqa: PLE0704

    view = self.index.targets_view(
        targets, recursive=recursive, with_deps=with_deps, onerror=onerror
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
            "failed to create '%s' from '%s'", dest_path, src_path, exc_info=True
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
    for key, typ in out_changes.items():
        out_path = self.fs.join(self.root_dir, *key)

        if out_path in failed:
            self.fs.remove(out_path, recursive=True)
        else:
            self.state.save_link(out_path, self.fs)
            stats[typ_map[typ]].append(_fspath_dir(out_path))

    if failed and not allow_missing:
        raise CheckoutError([relpath(out_path) for out_path in failed], stats)

    return stats
