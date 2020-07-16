import os


class BaseTree:
    """Abstract class to represent access to files"""

    @property
    def tree_root(self):
        pass

    def open(self, path, mode="r", encoding="utf-8"):
        """Open file and return a stream."""

    def exists(self, path):
        """Test whether a path exists."""

    def isdir(self, path):
        """Return true if the pathname refers to an existing directory."""

    def isfile(self, path):
        """Test whether a path is a regular file"""

    def walk(self, top, topdown=True, onerror=None):
        """Directory tree generator.

        See `os.walk` for the docs. Differences:
        - no support for symlinks
        """

    def walk_files(self, top):
        for root, _, files in self.walk(top):
            for file in files:
                # NOTE: os.path.join is ~5.5 times slower
                yield f"{root}{os.sep}{file}"

    def makedirs(self, path, mode=0o777, exist_ok=True):
        raise NotImplementedError


def is_working_tree(tree):
    from dvc.tree.local import LocalRemoteTree

    return isinstance(tree, LocalRemoteTree) or isinstance(
        getattr(tree, "tree", None), LocalRemoteTree
    )
