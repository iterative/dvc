import os

from dvc.exceptions import DvcException
from dvc.repo import locked
from dvc.scm.git import Git


@locked
def diff(self, a_rev="HEAD", b_rev=None):
    """
    By default, it compares the working tree with the last commit's tree.

    This implementation differs from `git diff` since DVC doesn't have
    the concept of `index`, but it keeps the same interface, thus,
    `dvc diff` would be the same as `dvc diff HEAD`.
    """
    if type(self.scm) is not Git:
        raise DvcException("only supported for Git repositories")

    def _paths_checksums():
        """
        A dictionary of checksums addressed by relpaths collected from
        the current tree outputs.

        Unpack directories to include their entries

        To help distinguish between a directory and a file output,
        the former one will come with a trailing slash in the path:

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
                    path = str(output.path_info / entry["relpath"])
                    result.update({path: entry["md5"]})

        return result

    working_tree = self.tree
    a_tree = self.scm.get_tree(a_rev)
    b_tree = self.scm.get_tree(b_rev) if b_rev else working_tree

    try:
        self.tree = a_tree
        old = _paths_checksums()

        self.tree = b_tree
        new = _paths_checksums()
    finally:
        self.tree = working_tree

    # Compare paths between the old and new tree.
    # set() efficiently converts dict keys to a set
    added = sorted(set(new) - set(old))
    deleted = sorted(set(old) - set(new))
    modified = sorted(set(old) & set(new))

    return {
        "added": [{"path": path, "checksum": new[path]} for path in added],
        "deleted": [{"path": path, "checksum": old[path]} for path in deleted],
        "modified": [
            {"path": path, "checksum": {"old": old[path], "new": new[path]}}
            for path in modified
            if old[path] != new[path]
        ],
    }
