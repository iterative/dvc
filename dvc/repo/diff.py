import errno
import logging
import os
from collections import defaultdict
from typing import Dict, List, Optional

from dvc.repo import locked
from dvc.ui import ui

logger = logging.getLogger(__name__)


def _path(entry):
    if entry and entry.meta and entry.meta.isdir:
        return os.path.join(*entry.key, "")
    return os.path.join(*entry.key)


def _hash(entry):
    if entry and entry.hash_info:
        return entry.hash_info.value
    return None


def _diff(old, new, with_missing=False):
    from dvc_data.index.diff import ADD, DELETE, MODIFY, RENAME
    from dvc_data.index.diff import diff as idiff

    ret: "Dict[str, List[Dict]]" = {
        "added": [],
        "deleted": [],
        "modified": [],
        "renamed": [],
        "not in cache": [],
    }

    for change in idiff(
        old,
        new,
        with_renames=True,
        hash_only=True,
    ):
        if change.typ == ADD:
            ret["added"].append(
                {
                    "path": _path(change.new),
                    "hash": _hash(change.new),
                }
            )
        elif change.typ == DELETE:
            ret["deleted"].append(
                {
                    "path": _path(change.old),
                    "hash": _hash(change.old),
                }
            )
        elif change.typ == MODIFY:
            ret["modified"].append(
                {
                    "path": _path(change.old),
                    "hash": {
                        "old": _hash(change.old),
                        "new": _hash(change.new),
                    },
                }
            )
        elif change.typ == RENAME:
            ret["renamed"].append(
                {
                    "path": {
                        "old": _path(change.old),
                        "new": _path(change.new),
                    },
                    "hash": _hash(change.old),
                }
            )

        if (
            with_missing
            and change.old
            and change.old.hash_info
            and not old.storage_map.cache_exists(change.old)
        ):
            ret["not in cache"].append(
                {
                    "path": _path(change.old),
                    "hash": _hash(change.old),
                }
            )

    return ret if any(ret.values()) else {}


@locked
def diff(
    self,
    a_rev: str = "HEAD",
    b_rev: Optional[str] = None,
    targets: Optional[List[str]] = None,
    recursive: bool = False,
):
    """
    By default, it compares the workspace with the last commit's fs.

    This implementation differs from `git diff` since DVC doesn't have
    the concept of `index`, but it keeps the same interface, thus,
    `dvc diff` would be the same as `dvc diff HEAD`.
    """
    if self.scm.no_commits:
        return {}

    indexes = {}
    missing_targets = defaultdict(set)
    with_missing = False
    if not b_rev:
        b_rev = "workspace"
        with_missing = True

    for rev in self.brancher(revs=[a_rev, b_rev]):
        if rev == "workspace" and b_rev != "workspace":
            # brancher always returns workspace, but we only need to compute
            # workspace paths/checksums if b_rev was None
            continue

        def onerror(target, _exc):
            # pylint: disable-next=cell-var-from-loop
            missing_targets[rev].add(target)  # noqa: B023

        view = self.index.targets_view(
            targets,
            onerror=onerror,
            recursive=recursive,
        )

        if rev == "workspace":
            from .index import build_data_index

            with ui.status("Building workspace index"):
                data = build_data_index(
                    view,
                    self.root_dir,
                    self.fs,
                    compute_hash=True,
                )
        else:
            data = view.data["repo"]

        assert rev not in indexes
        indexes[rev] = data

    if targets:
        old_missing = missing_targets.get(a_rev, set())
        new_missing = missing_targets.get(b_rev, set())

        # check for overlapping missing targets between a_rev and b_rev
        for target in old_missing & new_missing:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), target)

    if len(indexes.keys()) == 1:
        # both a_rev and b_rev point to the same sha, nothing to compare
        old = None
        new = None
    else:
        old = indexes[a_rev]
        new = indexes[b_rev]

    with ui.status("Calculating diff"):
        return _diff(old, new, with_missing=with_missing)
