import logging
import os
import re
from collections import namedtuple
from itertools import groupby, takewhile

from pathspec.patterns import GitWildMatchPattern
from pathspec.util import normalize_file

from dvc.path_info import PathInfo
from dvc.pathspec_math import PatternInfo, merge_patterns
from dvc.system import System
from dvc.utils import relpath
from dvc.utils.collections import PathStringTrie

logger = logging.getLogger(__name__)


class DvcIgnore:
    DVCIGNORE_FILE = ".dvcignore"

    def __call__(self, root, dirs, files):
        raise NotImplementedError


class DvcIgnorePatterns(DvcIgnore):
    def __init__(self, pattern_list, dirname):
        if pattern_list:
            if isinstance(pattern_list[0], str):
                pattern_list = [
                    PatternInfo(pattern, "") for pattern in pattern_list
                ]

        self.pattern_list = pattern_list
        self.dirname = dirname
        self.prefix = self.dirname + os.sep

        self.regex_pattern_list = [
            GitWildMatchPattern.pattern_to_regex(pattern_info.patterns)
            for pattern_info in pattern_list
        ]

        self.ignore_spec = [
            (ignore, re.compile("|".join(item[0] for item in group)))
            for ignore, group in groupby(
                self.regex_pattern_list, lambda x: x[1]
            )
            if ignore is not None
        ]

    @classmethod
    def from_files(cls, ignore_file_path, tree):
        assert os.path.isabs(ignore_file_path)
        dirname = os.path.normpath(os.path.dirname(ignore_file_path))
        ignore_file_rel_path = os.path.relpath(
            ignore_file_path, tree.tree_root
        )
        with tree.open(ignore_file_path, encoding="utf-8") as fobj:
            path_spec_lines = [
                PatternInfo(
                    line,
                    "{}:{}:{}".format(ignore_file_rel_path, line_no + 1, line),
                )
                for line_no, line in enumerate(
                    map(str.strip, fobj.readlines())
                )
                if line and not (line.strip().startswith("#"))
            ]

        return cls(path_spec_lines, dirname)

    def __call__(self, root, dirs, files):
        files = [f for f in files if not self.matches(root, f)]
        dirs = [d for d in dirs if not self.matches(root, d, True)]

        return dirs, files

    def _get_normalize_path(self, dirname, basename):
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
        return path

    def matches(self, dirname, basename, is_dir=False):
        path = self._get_normalize_path(dirname, basename)
        if not path:
            return False
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

    def match_details(self, dirname, basename, is_dir=False):
        path = self._get_normalize_path(dirname, basename)
        if not path:
            return False
        return self._ignore_details(path, is_dir)

    def _ignore_details(self, path, is_dir):
        result = []
        for ignore, pattern in zip(self.regex_pattern_list, self.pattern_list):
            regex = re.compile(ignore[0])
            # skip system pattern
            if not pattern.file_info:
                continue
            if is_dir:
                path_dir = f"{path}/"
                if regex.match(path) or regex.match(path_dir):
                    result.append(pattern.file_info)
            else:
                if regex.match(path):
                    result.append(pattern.file_info)
        return result

    def __hash__(self):
        return hash(self.dirname + ":" + str(self.pattern_list))

    def __eq__(self, other):
        if not isinstance(other, DvcIgnorePatterns):
            return NotImplemented
        return (self.dirname == other.dirname) & (
            [pattern.patterns for pattern in self.pattern_list]
            == [pattern.patterns for pattern in other.pattern_list]
        )

    def __bool__(self):
        return bool(self.pattern_list)


CheckIgnoreResult = namedtuple(
    "CheckIgnoreResult", ["file", "match", "patterns"]
)


def _no_match(path):
    return CheckIgnoreResult(path, False, ["::"])


class DvcIgnoreFilterNoop:
    def __init__(self, tree, root_dir):
        pass

    def __call__(self, root, dirs, files, **kwargs):
        return dirs, files

    def is_ignored_dir(self, _):
        return False

    def is_ignored_file(self, _):
        return False

    def check_ignore(self, path):
        return _no_match(path)

    def is_ignored(self, _):
        return False


class DvcIgnoreFilter:
    @staticmethod
    def _is_dvc_repo(root, directory):
        from dvc.repo import Repo

        return os.path.isdir(os.path.join(root, directory, Repo.DVC_DIR))

    def __init__(self, tree, root_dir):
        from dvc.repo import Repo

        default_ignore_patterns = [".hg/", ".git/", "{}/".format(Repo.DVC_DIR)]

        self.tree = tree
        self.root_dir = root_dir
        self.ignores_trie_tree = PathStringTrie()
        self.ignores_trie_tree[root_dir] = DvcIgnorePatterns(
            default_ignore_patterns, root_dir
        )
        self._ignored_subrepos = PathStringTrie()
        self._update(self.root_dir)

    def _update(self, dirname):
        old_pattern = self.ignores_trie_tree.longest_prefix(dirname).value
        matches = old_pattern.matches(dirname, DvcIgnore.DVCIGNORE_FILE, False)

        ignore_file_path = os.path.join(dirname, DvcIgnore.DVCIGNORE_FILE)
        if not matches and self.tree.exists(
            ignore_file_path, use_dvcignore=False
        ):
            new_pattern = DvcIgnorePatterns.from_files(
                ignore_file_path, self.tree
            )
            if old_pattern:
                self.ignores_trie_tree[dirname] = DvcIgnorePatterns(
                    *merge_patterns(
                        old_pattern.pattern_list,
                        old_pattern.dirname,
                        new_pattern.pattern_list,
                        new_pattern.dirname,
                    )
                )
            else:
                self.ignores_trie_tree[dirname] = new_pattern
        elif old_pattern:
            self.ignores_trie_tree[dirname] = old_pattern

        # NOTE: using `walk` + `break` because tree doesn't have `listdir()`
        for root, dirs, _ in self.tree.walk(dirname, use_dvcignore=False):
            self._update_sub_repo(root, dirs)
            break

    def _update_sub_repo(self, root, dirs):
        for d in dirs:
            if self._is_dvc_repo(root, d):
                self._ignored_subrepos[root] = self._ignored_subrepos.get(
                    root, set()
                ) | {d}
                pattern_info = PatternInfo(
                    f"/{d}/", "in sub_repo:{}".format(d)
                )
                new_pattern = DvcIgnorePatterns([pattern_info], root)
                old_pattern = self.ignores_trie_tree.longest_prefix(root).value
                if old_pattern:
                    self.ignores_trie_tree[root] = DvcIgnorePatterns(
                        *merge_patterns(
                            old_pattern.pattern_list,
                            old_pattern.dirname,
                            new_pattern.pattern_list,
                            new_pattern.dirname,
                        )
                    )
                else:
                    self.ignores_trie_tree[root] = new_pattern

    def __call__(self, root, dirs, files, ignore_subrepos=True):
        ignore_pattern = self._get_trie_pattern(root)
        if ignore_pattern:
            dirs, files = ignore_pattern(root, dirs, files)
            if not ignore_subrepos:
                dirs.extend(self._ignored_subrepos.get(root, []))
        return dirs, files

    def _get_trie_pattern(self, dirname):
        ignore_pattern = self.ignores_trie_tree.get(dirname)
        if ignore_pattern:
            return ignore_pattern

        prefix = self.ignores_trie_tree.longest_prefix(dirname).key
        if not prefix:
            # outside of the repo
            return None

        dirs = list(
            takewhile(
                lambda path: path != prefix,
                (parent.fspath for parent in PathInfo(dirname).parents),
            )
        )
        dirs.reverse()
        dirs.append(dirname)

        for parent in dirs:
            self._update(parent)

        return self.ignores_trie_tree.get(dirname)

    def _is_ignored(self, path, is_dir=False):
        if self._outside_repo(path):
            return False
        dirname, basename = os.path.split(os.path.normpath(path))
        ignore_pattern = self._get_trie_pattern(dirname)
        if ignore_pattern:
            return ignore_pattern.matches(dirname, basename, is_dir)
        else:
            return False

    def _is_subrepo(self, path):
        dirname, basename = os.path.split(os.path.normpath(path))
        return basename in self._ignored_subrepos.get(dirname, set())

    def is_ignored_dir(self, path, ignore_subrepos=True):
        path = os.path.abspath(path)
        if not ignore_subrepos:
            return not self._is_subrepo(path)
        if path == self.root_dir:
            return False

        return self._is_ignored(path, True)

    def is_ignored_file(self, path):
        path = os.path.abspath(path)
        return self._is_ignored(path, False)

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

    def check_ignore(self, target):
        full_target = os.path.abspath(target)
        if not self._outside_repo(full_target):
            dirname, basename = os.path.split(os.path.normpath(full_target))
            pattern = self._get_trie_pattern(dirname)
            if pattern:
                matches = pattern.match_details(
                    dirname, basename, os.path.isdir(full_target)
                )

                if matches:
                    return CheckIgnoreResult(target, True, matches)
        return _no_match(target)

    def is_ignored(self, path):
        # NOTE: can't use self.check_ignore(path).match for now, see
        # https://github.com/iterative/dvc/issues/4555
        return self.is_ignored_dir(path) or self.is_ignored_file(path)


def init(path):
    dvcignore = os.path.join(path, DvcIgnore.DVCIGNORE_FILE)
    if os.path.exists(dvcignore):
        return dvcignore

    with open(dvcignore, "w") as fobj:
        fobj.write(
            "# Add patterns of files dvc should ignore, which could improve\n"
            "# the performance. Learn more at\n"
            "# https://dvc.org/doc/user-guide/dvcignore\n"
        )

    return dvcignore
