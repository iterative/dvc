import logging
import os
from typing import Dict, List

from dvc.exceptions import CheckoutError, CheckoutErrorSuggestGit, NoOutputOrStageError
from dvc.utils import relpath

from . import locked

logger = logging.getLogger(__name__)


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
    for objects in changes.values():
        for change in objects:
            for out_key in out_keys:
                if (
                    len(out_key) > len(change.key)
                    or change.key[: len(out_key)] != out_key
                ):
                    continue

                if change.key == out_key:
                    out_changes[out_key] = change.typ
                elif not out_changes.get(out_key):
                    out_changes[out_key] = MODIFY
                break

    return out_changes


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
    from dvc import prompt
    from dvc.fs.callbacks import Callback
    from dvc.repo.index import build_data_index
    from dvc.stage.exceptions import StageFileBadNameError, StageFileDoesNotExistError
    from dvc_data.hashfile.checkout import CheckoutError as IndexCheckoutError
    from dvc_data.index.checkout import ADD, DELETE, MODIFY
    from dvc_data.index.checkout import checkout as icheckout

    stats: Dict[str, List[str]] = {
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
            (
                StageFileDoesNotExistError,
                StageFileBadNameError,
                NoOutputOrStageError,
            ),
        ):
            raise CheckoutErrorSuggestGit(target) from exc
        raise  # pylint: disable=misplaced-bare-raise

    view = self.index.targets_view(
        targets,
        recursive=recursive,
        with_deps=with_deps,
        onerror=onerror,
    )

    with Callback.as_tqdm_callback(
        unit="entry",
        desc="Building data index",
    ) as cb:
        old = build_data_index(
            view, self.root_dir, self.fs, compute_hash=True, callback=cb
        )

    new = view.data["repo"]

    with Callback.as_tqdm_callback(
        unit="file",
        desc="Checkout",
    ) as cb:
        try:
            changes = icheckout(
                new,
                self.root_dir,
                self.fs,
                old=old,
                callback=cb,
                delete=True,
                prompt=prompt.confirm,
                update_meta=False,
                relink=relink,
                force=force,
                allow_missing=allow_missing,
                state=self.state,
                **kwargs,
            )
        except IndexCheckoutError as exc:
            raise CheckoutError(exc.paths, {}) from exc

    out_changes = _build_out_changes(view, changes)

    typ_map = {ADD: "added", DELETE: "deleted", MODIFY: "modified"}
    for key, typ in out_changes.items():
        out_path = self.fs.path.join(self.root_dir, *key)
        self.state.save_link(out_path, self.fs)
        stats[typ_map[typ]].append(_fspath_dir(out_path))

    return stats
