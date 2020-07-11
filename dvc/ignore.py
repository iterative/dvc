import logging
import os
import re
from itertools import groupby

from funcy import cached_property
from pathspec.patterns import GitWildMatchPattern
from pathspec.util import normalize_file
from pygtrie import StringTrie

from dvc.path_info import PathInfo
from dvc.pathspec_math import merge_patterns
from dvc.scm.tree import BaseTree
from dvc.system import System
from dvc.utils import relpath

logger = logging.getLogger(__name__)


class DvcIgnore:
    DVCIGNORE_FILE = ".dvcignore"

    def __call__(self, root, dirs, files):
        raise NotImplementedError


class DvcIgnorePatterns(DvcIgnore):
    def __init__(self, pattern_list, dirname):

        self.pattern_list = pattern_list
        self.dirname = dirname
        self.prefix = self.dirname + os.sep

        regex_pattern_list = map(
            GitWildMatchPattern.pattern_to_regex, pattern_list
        )

        self.ignore_spec = [
            (ignore, re.compile("|".join(item[0] for item in group)))
            for ignore, group in groupby(regex_pattern_list, lambda x: x[1])
            if ignore is not None
        ]

    @classmethod
    def from_files(cls, ignore_file_path, tree):
        assert os.path.isabs(ignore_file_path)
        dirname = os.path.normpath(os.path.dirname(ignore_file_path))
        with tree.open(ignore_file_path, encoding="utf-8") as fobj:
            path_spec_lines = [
                line for line in map(str.strip, fobj.readlines()) if line
            ]

        return cls(path_spec_lines, dirname)

    def __call__(self, root, dirs, files):
        files = [f for f in files if not self.matches(root, f)]
        dirs = [d for d in dirs if not self.matches(root, d, True)]

        return dirs, files

    def matches(self, dirname, basename, is_dir=False):
        # NOTE: `relpath` is too slow, so we have to assume that both
        # `dirname` and `self.dirname` are relative or absolute together.
        if dirname == self.dirname:
            path = basename
        elif dirname.startswith(self.prefix):
            rel = dirname[len(self.prefix) :]
            # NOTE: `os.path.join` is ~x5.5 slower
            path = f"{rel}{os.sep}{basename}"
        else:
            return False

        if not System.is_unix():
            path = normalize_file(path)
        return self.ignore(path, is_dir)

    def ignore(self, path, is_dir):
        result = False
        if is_dir:
            path_dir = f"{path}/"
            for ignore, pattern in self.ignore_spec:
                if pattern.match(path) or pattern.match(path_dir):
                    result = ignore
        else:
            for ignore, pattern in self.ignore_spec:
                if pattern.match(path):
                    result = ignore
        return result

    def __hash__(self):
        return hash(self.dirname + ":" + "\n".join(self.pattern_list))

    def __eq__(self, other):
        if not isinstance(other, DvcIgnorePatterns):
            return NotImplemented
        return (self.dirname == other.dirname) & (
            self.pattern_list == other.pattern_list
        )

    def __bool__(self):
        return bool(self.pattern_list)


class DvcIgnorePatternsTrie(DvcIgnore):
    trie = None

    def __init__(self):
        if self.trie is None:
            self.trie = StringTrie(separator=os.sep)

    def __new__(cls, *args, **kwargs):
        if not hasattr(DvcIgnorePatterns, "_instance"):
            if not hasattr(DvcIgnorePatterns, "_instance"):
                DvcIgnorePatterns._instance = object.__new__(cls)
        return DvcIgnorePatterns._instance

    def __call__(self, root, dirs, files):
        ignore_pattern = self[root]
        if ignore_pattern:
            return ignore_pattern(root, dirs, files)
        return dirs, files

    def __setitem__(self, root, ignore_pattern):
        base_pattern = self[root]
        common_dirname, merged_pattern = merge_patterns(
            base_pattern.dirname,
            base_pattern.pattern_list,
            ignore_pattern.dirname,
            ignore_pattern.pattern_list,
        )
        self.trie[root] = DvcIgnorePatterns(merged_pattern, common_dirname)

    def __getitem__(self, root):
        ignore_pattern = self.trie.longest_prefix(root)
        if ignore_pattern:
            return ignore_pattern.value
        return DvcIgnorePatterns([], root)


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


class DvcIgnoreRepo(DvcIgnore):
    def __call__(self, root, dirs, files):
        def is_dvc_repo(directory):
            from dvc.repo import Repo

            return os.path.isdir(os.path.join(root, directory, Repo.DVC_DIR))

        dirs = [d for d in dirs if not is_dvc_repo(d)]

        return dirs, files


class DvcIgnoreFilter:
    def __init__(self, tree, root_dir):
        self.tree = tree
        self.root_dir = root_dir
        self.ignores = {
            DvcIgnoreDirs([".git", ".hg", ".dvc"]),
            DvcIgnoreRepo(),
        }
        for root, dirs, _ in self.tree.walk(self.root_dir):
            self._update(root)
            dirs[:], _ = self(root, dirs, [])

    def _update(self, dirname):
        ignore_file_path = os.path.join(dirname, DvcIgnore.DVCIGNORE_FILE)
        if self.tree.exists(ignore_file_path):
            ignore_pattern = DvcIgnorePatterns.from_files(
                ignore_file_path, self.tree
            )
            ignore_pattern_trie = DvcIgnorePatternsTrie()
            ignore_pattern_trie[dirname] = ignore_pattern
            self.ignores.add(ignore_pattern_trie)

    def __call__(self, root, dirs, files):
        for ignore in self.ignores:
            dirs, files = ignore(root, dirs, files)

        return dirs, files


class CleanTree(BaseTree):
    def __init__(self, tree, tree_root=None):
        self.tree = tree
        if tree_root:
            self._tree_root = tree_root
        else:
            self._tree_root = self.tree.tree_root

    @cached_property
    def dvcignore(self):
        return DvcIgnoreFilter(self.tree, self.tree_root)

    @property
    def tree_root(self):
        return self._tree_root

    def open(self, path, mode="r", encoding="utf-8"):
        if self.isfile(path):
            return self.tree.open(path, mode, encoding)
        raise FileNotFoundError

    def exists(self, path):
        if self.tree.exists(path) and self._parents_exist(path):
            if self.tree.isdir(path):
                return self._valid_dirname(path)
            return self._valid_filename(path)
        return False

    def isdir(self, path):
        return (
            self.tree.isdir(path)
            and self._parents_exist(path)
            and self._valid_dirname(path)
        )

    def _valid_dirname(self, path):
        path = os.path.abspath(path)
        if path == self.tree_root:
            return True
        dirname, basename = os.path.split(path)
        dirs, _ = self.dvcignore(dirname, [basename], [])
        if dirs:
            return True
        return False

    def isfile(self, path):
        return (
            self.tree.isfile(path)
            and self._parents_exist(path)
            and self._valid_filename(path)
        )

    def _valid_filename(self, path):
        dirname, basename = os.path.split(os.path.normpath(path))
        _, files = self.dvcignore(os.path.abspath(dirname), [], [basename])
        if files:
            return True
        return False

    def isexec(self, path):
        return self.exists(path) and self.tree.isexec(path)

    def _parents_exist(self, path):
        from dvc.repo import Repo

        path = PathInfo(path)

        # if parent is tree_root or inside a .dvc dir we can skip this check
        if path.parent == self.tree_root or Repo.DVC_DIR in path.parts:
            return True

        # paths outside of the CleanTree root should be ignored
        path = relpath(path, self.tree_root)
        if path.startswith("..") or (
            os.name == "nt"
            and not os.path.commonprefix(
                [os.path.abspath(path), self.tree_root]
            )
        ):
            return False

        # check if parent directories are in our ignores, starting from
        # tree_root
        for parent_dir in reversed(PathInfo(path).parents):
            dirname, basename = os.path.split(parent_dir)
            if basename == ".":
                # parent_dir == tree_root
                continue
            dirs, _ = self.dvcignore(os.path.abspath(dirname), [basename], [])
            if not dirs:
                return False
        return True

    def walk(self, top, topdown=True, onerror=None):
        for root, dirs, files in self.tree.walk(
            top, topdown=topdown, onerror=onerror
        ):
            dirs[:], files[:] = self.dvcignore(
                os.path.abspath(root), dirs, files
            )

            yield root, dirs, files

    def stat(self, path):
        if self.exists(path):
            return self.tree.stat(path)
        raise FileNotFoundError

    @property
    def hash_jobs(self):
        return self.tree.hash_jobs

    def relative_path(self, abspath):
        pass
