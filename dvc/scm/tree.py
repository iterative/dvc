import os
import stat
from multiprocessing import cpu_count

from funcy import cached_property


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


class WorkingTree(BaseTree):
    """Proxies the repo file access methods to working tree files"""

    def __init__(self, repo_root=None):
        repo_root = repo_root or os.getcwd()
        self.repo_root = repo_root

    @property
    def tree_root(self):
        return self.repo_root

    def open(self, path, mode="r", encoding="utf-8"):
        """Open file and return a stream."""
        if "b" in mode:
            encoding = None
        return open(path, mode=mode, encoding=encoding)

    def exists(self, path):
        """Test whether a path exists."""
        return os.path.lexists(path)

    def isdir(self, path):
        """Return true if the pathname refers to an existing directory."""
        return os.path.isdir(path)

    def isfile(self, path):
        """Test whether a path is a regular file"""
        return os.path.isfile(path)

    def walk(self, top, topdown=True, onerror=None):
        """Directory tree generator.

        See `os.walk` for the docs. Differences:
        - no support for symlinks
        """
        for root, dirs, files in os.walk(
            top, topdown=topdown, onerror=onerror
        ):
            yield os.path.normpath(root), dirs, files

    def isexec(self, path):
        mode = os.stat(path).st_mode
        return mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    @staticmethod
    def stat(path):
        return os.stat(path)

    @cached_property
    def hash_jobs(self):
        return max(1, min(4, cpu_count() // 2))


def is_working_tree(tree):
    return isinstance(tree, WorkingTree) or isinstance(
        getattr(tree, "tree", None), WorkingTree
    )
