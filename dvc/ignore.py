import logging
import os

from funcy import cached_property
from pathspec import PathSpec
from pathspec.patterns import GitWildMatchPattern

from dvc.scm.tree import BaseTree
from dvc.utils import relpath

logger = logging.getLogger(__name__)


class DvcIgnore(object):
    DVCIGNORE_FILE = ".dvcignore"

    def __call__(self, root, dirs, files):
        raise NotImplementedError


class DvcIgnorePatterns(DvcIgnore):
    def __init__(self, ignore_file_path, tree):
        assert os.path.isabs(ignore_file_path)

        self.ignore_file_path = ignore_file_path
        self.dirname = os.path.normpath(os.path.dirname(ignore_file_path))

        with tree.open(ignore_file_path, encoding="utf-8") as fobj:
            self.ignore_spec = PathSpec.from_lines(GitWildMatchPattern, fobj)

    def __call__(self, root, dirs, files):
        files = [f for f in files if not self.matches(root, f)]
        dirs = [d for d in dirs if not self.matches(root, d)]

        return dirs, files

    def matches(self, dirname, basename):
        abs_path = os.path.join(dirname, basename)
        rel_path = relpath(abs_path, self.dirname)

        if os.pardir + os.sep in rel_path:
            return False
        return self.ignore_spec.match_file(rel_path)

    def __hash__(self):
        return hash(self.ignore_file_path)

    def __eq__(self, other):
        if not isinstance(other, DvcIgnorePatterns):
            return NotImplemented

        return self.ignore_file_path == other.ignore_file_path


class DvcIgnoreDirs(DvcIgnore):
    def __init__(self, basenames):
        self.basenames = set(basenames)

    def __call__(self, root, dirs, files):
        dirs = [d for d in dirs if d not in self.basenames]

        return dirs, files

    def __hash__(self):
        return hash(tuple(self.basenames))

    def __eq__(self, other):
        if not isinstance(other, DvcIgnoreDirs):
            return NotImplemented

        return self.basenames == other.basenames


class DvcIgnoreFilter(object):
    def __init__(self, tree):
        self.tree = tree
        self.ignores = {DvcIgnoreDirs([".git", ".hg", ".dvc"])}
        for root, dirs, files in self.tree.walk(self.tree.tree_root):
            self._update(root)
            dirs[:], files[:] = self(root, dirs, files)

    def _update(self, dirname):
        ignore_file_path = os.path.join(dirname, DvcIgnore.DVCIGNORE_FILE)
        if self.tree.exists(ignore_file_path):
            self.ignores.add(DvcIgnorePatterns(ignore_file_path, self.tree))

    def __call__(self, root, dirs, files):
        for ignore in self.ignores:
            dirs, files = ignore(root, dirs, files)

        return dirs, files


class CleanTree(BaseTree):
    def __init__(self, tree):
        self.tree = tree

    @cached_property
    def dvcignore(self):
        return DvcIgnoreFilter(self.tree)

    @property
    def tree_root(self):
        return self.tree.tree_root

    def open(self, path, mode="r", encoding="utf-8"):
        return self.tree.open(path, mode, encoding)

    def exists(self, path):
        return self.tree.exists(path)

    def isdir(self, path):
        return self.tree.isdir(path)

    def isfile(self, path):
        return self.tree.isfile(path)

    def walk(self, top, topdown=True):
        for root, dirs, files in self.tree.walk(top, topdown):
            dirs[:], files[:] = self.dvcignore(root, dirs, files)

            yield root, dirs, files

    def walk_files(self, top):
        for root, _, files in self.walk(top):
            for file in files:
                yield os.path.join(root, file)
