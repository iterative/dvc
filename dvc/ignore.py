import logging
import os
import re
from itertools import groupby

from pathspec.patterns import GitWildMatchPattern
from pathspec.util import normalize_file
from pygtrie import StringTrie

from dvc.path_info import PathInfo
from dvc.pathspec_math import merge_patterns
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
    def __init__(self, default_ignore_pattern, root):
        self.trie = StringTrie(separator=os.sep)
        self.trie[root] = DvcIgnorePatterns(default_ignore_pattern, root)

    def __call__(self, root, dirs, files):
        ignore_pattern = self[root]
        if ignore_pattern:
            return ignore_pattern(root, dirs, files)
        return dirs, files

    def __setitem__(self, root, ignore_pattern):
        base_pattern = self[root]
        self.trie[root] = DvcIgnorePatterns(
            *merge_patterns(
                base_pattern.pattern_list,
                base_pattern.dirname,
                ignore_pattern.pattern_list,
                ignore_pattern.dirname,
            )
        )

    def __getitem__(self, root):
        ignore_pattern = self.trie.longest_prefix(root)
        return ignore_pattern.value


class DvcIgnoreFilterNoop:
    def __init__(self, tree, root_dir):
        pass

    def __call__(self, root, dirs, files):
        return dirs, files

    def is_ignored_dir(self, _):
        return False

    def is_ignored_file(self, _):
        return False


class DvcIgnoreFilter:
    def __init__(self, tree, root_dir):
        from dvc.repo import Repo

        self.tree = tree
        self.root_dir = root_dir
        default_ignore_pattern = [".hg/", ".git/", "{}/".format(Repo.DVC_DIR)]
        self.ignores_trie_tree = DvcIgnorePatternsTrie(
            default_ignore_pattern, root_dir
        )
        for root, dirs, _ in self.tree.walk(self.root_dir):
            self._update(root)
            dirs[:], _ = self(root, dirs, [])

    def _update(self, dirname):
        ignore_file_path = os.path.join(dirname, DvcIgnore.DVCIGNORE_FILE)
        if self.tree.exists(ignore_file_path):
            self.ignores_trie_tree[dirname] = DvcIgnorePatterns.from_files(
                ignore_file_path, self.tree
            )

    def __call__(self, root, dirs, files):
        return self.ignores_trie_tree(root, dirs, files)

    def is_ignored_dir(self, path):
        if self._outside_repo(path):
            return True

        path = os.path.abspath(path)
        if path == self.root_dir:
            return False
        dirname, basename = os.path.split(path)
        dirs, _ = self(dirname, [basename], [])
        return not dirs

    def is_ignored_file(self, path):
        if self._outside_repo(path):
            return True

        dirname, basename = os.path.split(os.path.normpath(path))
        _, files = self(os.path.abspath(dirname), [], [basename])
        return not files

    def _outside_repo(self, path):
        path = PathInfo(path)

        # paths outside of the repo should be ignored
        path = relpath(path, self.root_dir)
        if path.startswith("..") or (
            os.name == "nt"
            and not os.path.commonprefix(
                [os.path.abspath(path), self.root_dir]
            )
        ):
            return True
        return False
