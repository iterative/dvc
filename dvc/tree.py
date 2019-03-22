import os

from dvc.utils.compat import open


class BaseTree(object):
    """Abstract class to represent access to files"""

    def open(self, path):
        """Open file and return a stream."""

    def exists(self, path):
        """Test whether a path exists."""

    def isdir(self, path):
        """Return true if the pathname refers to an existing directory."""

    def isfile(self, path):
        """Test whether a path is a regular file"""

    def walk(self, top, topdown=True):
        """Directory tree generator.

        See `os.walk` for the docs. Differences:
        - no support for symlinks
        - it could raise exceptions, there is no onerror argument
        """


class WorkingTree(BaseTree):
    """Proxies the repo file access methods to working tree files"""

    def open(self, path):
        """Open file and return a stream."""
        return open(path, encoding="utf-8")

    def exists(self, path):
        """Test whether a path exists."""
        return os.path.exists(path)

    def isdir(self, path):
        """Return true if the pathname refers to an existing directory."""
        return os.path.isdir(path)

    def isfile(self, path):
        """Test whether a path is a regular file"""
        return os.path.isfile(path)

    def walk(self, top, topdown=True):
        """Directory tree generator.

        See `os.walk` for the docs. Differences:
        - no support for symlinks
        - it could raise exceptions, there is no onerror argument
        """

        def onerror(e):
            raise e

        for root, dirs, files in os.walk(
            top, topdown=topdown, onerror=onerror
        ):
            if topdown:
                dirs[:] = [i for i in dirs if i not in (".git", ".hg", ".dvc")]
            yield os.path.normpath(root), dirs, files
