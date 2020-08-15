import os

from dvc.repo import locked
from dvc.tree.local import LocalTree


@locked
def diff(self, a_rev="HEAD", b_rev=None):
    """
    By default, it compares the workspace with the last commit's tree.

    This implementation differs from `git diff` since DVC doesn't have
    the concept of `index`, but it keeps the same interface, thus,
    `dvc diff` would be the same as `dvc diff HEAD`.
    """

    def _paths_checksums():
        """
        A dictionary of checksums addressed by relpaths collected from
        the current tree outputs.

        To help distinguish between a directory and a file output,
        the former one will come with a trailing slash in the path:

            directory: "data/"
            file:      "data"
        """

        def _to_path(output):
            return (
                str(output)
                if not output.is_dir_checksum
                else os.path.join(str(output), "")
            )

        on_working_tree = isinstance(self.tree, LocalTree)

        def _to_checksum(output):
            if on_working_tree:
                return self.cache.local.tree.get_hash(output.path_info)[1]
            return output.checksum

        def _exists(output):
            if on_working_tree:
                return output.exists
            return True

        return {
            _to_path(output): _to_checksum(output)
            for stage in self.stages
            for output in stage.outs
            if _exists(output)
        }

    if self.scm.no_commits:
        return {}

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

    ret = {
        "added": [{"path": path, "hash": new[path]} for path in added],
        "deleted": [{"path": path, "hash": old[path]} for path in deleted],
        "modified": [
            {"path": path, "hash": {"old": old[path], "new": new[path]}}
            for path in modified
            if old[path] != new[path]
        ],
    }

    return ret if any(ret.values()) else {}
