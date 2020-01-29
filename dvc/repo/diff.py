import collections
import os

from dvc.exceptions import DvcException
from dvc.repo import locked
from dvc.scm.git import Git


@locked
def diff(self, a_ref="HEAD", b_ref=None):
    """
    By default, it compares the working tree with the last commit's tree.

    This implementation differs from `git diff` since DVC doesn't have
    the concept of `index`, but it keeps the same interface, thus,
    `dvc diff` would be the same as `dvc diff HEAD`.
    """
    if type(self.scm) is not Git:
        raise DvcException("only supported for Git repositories")


    def _checksums_by_filenames():
        """
        A dictionary of checksums addressed by filenames collected from
        the current tree outputs.

        Unpack directories to include their entries

        To help distinguish between a directory and a file output,
        the former one will come with a trailing slash in the filename:

            directory: "data/"
            file:      "data"
        """
        result = {}

        for stage in self.stages:
            for output in stage.outs:
                if not output.is_dir_checksum:
                    result.update({str(output): output.checksum})
                    continue

                result.update({os.path.join(str(output), ""): output.checksum})

                for entry in output.dir_cache:
                    filename = str(output.path_info / entry["relpath"])
                    result.update({filename: entry["md5"]})

        return result


    def _compare_trees(a_ref, b_ref):
        working_tree = self.tree

        a_tree = self.scm.get_tree(self.scm.resolve_rev(a_ref))
        b_tree = self.scm.get_tree(self.scm.resolve_rev(b_ref)) if b_tree else working_tree

    breakpoint()

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
