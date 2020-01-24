import os
import collections

from dvc.repo import locked
from dvc.compat import fspath


Diffable = collections.namedtuple("Diffable", "filename, checksum, size")
Diffable.__doc__ = "Common interface for comparable entries."


def diffable_from_output(output):
    try:
        size = os.path.getsize(fspath(output.path_info))
    except FileNotFoundError:
        size = None

    return Diffable(filename=str(output), checksum=output.checksum, size=size)


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

    old &= delta
    new &= delta

    return {
        "old": [entry._asdict() for entry in old],
        "new": [entry._asdict() for entry in new],
    }
