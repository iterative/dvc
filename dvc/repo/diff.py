import os
import collections

from dvc.repo import locked
from dvc.compat import fspath


Diffable = collections.namedtuple("Diffable", "filename, checksum, size")
Diffable.__doc__ = "Common interface for comparable entries."
Diffable._asdict = lambda x: {"checksum": x.checksum, "size": x.size}


def diffable_from_output(output):
    try:
        size = os.path.getsize(fspath(output.path_info))
    except FileNotFoundError:
        size = None

    return Diffable(filename=str(output), checksum=output.checksum, size=size)


def compare_states(old, new):
    if old and new:
        try:
            size = new["size"] - old["size"]
        except KeyError:
            size = "unknown"
        return {"status": "modified", "size": size}

    if old and not new:
        return {"status": "deleted", "size": old.get("size", "unknown")}

    if not old and new:
        return {"status": "added", "size": new.get("size", "unknown")}


@locked
def diff(self, a_ref="HEAD", b_ref=None, *, target=None):
    """
    By default, it compares the working tree with the last commit's tree.

    When a `target` path is given, it only shows that file's comparison.

    This implementation differs from `git diff`, since DVC doesn't have
    the concept of `index`, `dvc diff` would be the same as `dvc diff HEAD`.
    """
    outs = {}

    for branch in self.brancher(revs=[a_ref, b_ref]):
        outs[branch] = set(
            diffable_from_output(out)
            for stage in self.stages
            for out in stage.outs
        )

    old = outs[a_ref]
    new = outs[b_ref or "working tree"]
    delta = old ^ new

    if not delta:
        return

    result = {}

    for entry in delta:
        result[entry.filename] = {
            "old": entry._asdict() if entry in old else {},
            "new": entry._asdict() if entry in new else {},
        }

    for filename, entry in result.items():
        entry.update({"diff": compare_states(entry["old"], entry["new"])})

    return result
