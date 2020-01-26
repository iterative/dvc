import collections
import os

from dvc.exceptions import DvcException
from dvc.repo import locked
from dvc.scm.git import Git


Diffable = collections.namedtuple("Diffable", "filename, checksum")
Diffable.__doc__ = "Common interface to compare outputs."
Diffable.__eq__ = lambda self, other: self.filename == other.filename
Diffable.__hash__ = lambda self: hash(self.filename)


def _diffables_from_output(output):
    """
    Transform an output into a list of Diffable objects so we can
    compare them lately.

    Unpack directories to include entries' Diffables.

    To help distinguish between an a directory output and a file output,
    the former one will come with a trailing slash in the filename:

        directory: "data/"
        file:      "data"

    You can also rely on the checksum to tell whether it was computed for
    a file or a directory as a whole.
    """
    if output.is_dir_checksum:
        return [
            Diffable(
                filename=os.path.join(str(output), ""),
                checksum=output.checksum,
            )
        ] + [
            Diffable(
                filename=str(output.path_info / entry["relpath"]),
                checksum=entry["md5"],
            )
            for entry in output.dir_cache
        ]

    return [Diffable(filename=str(output), checksum=output.checksum)]


@locked
def diff(self, a_ref="HEAD", b_ref=None, *, target=None):
    """
    By default, it compares the working tree with the last commit's tree.

    When a `target` path is given, it only shows that file's comparison.

    This implementation differs from `git diff` since DVC doesn't have
    the concept of `index`, but it keeps the same interface, thus,
    `dvc diff` would be the same as `dvc diff HEAD`.
    """
    if type(self.scm) is not Git:
        raise DvcException("only supported for Git repositories")

    outs = {}

    for branch in self.brancher(revs=[a_ref, b_ref]):
        outs[branch] = set(
            diffable
            for stage in self.stages
            for out in stage.outs
            for diffable in _diffables_from_output(out)
            if not target or target == str(out)
        )

    old = outs[a_ref]
    new = outs[b_ref or "working tree"]

    added = new - old
    deleted = old - new
    delta = old ^ new

    result = {
        "added": [entry._asdict() for entry in sorted(added)],
        "deleted": [entry._asdict() for entry in sorted(deleted)],
        "modified": [],
    }

    for _old, _new in zip(sorted(old - delta), sorted(new - delta)):
        if _old.checksum == _new.checksum:
            continue

        result["modified"].append(
            {
                "filename": _new.filename,
                "checksum": {"old": _old.checksum, "new": _new.checksum},
            }
        )

    return result
