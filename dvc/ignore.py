from __future__ import unicode_literals

import logging
import os
from pathspec import PathSpec
from pathspec.patterns import GitWildMatchPattern

logger = logging.getLogger(__name__)


class DvcIgnore(object):
    DVCIGNORE_FILE = ".dvcignore"

    def __call__(self, root, dirs, files):
        raise NotImplementedError


def full_path_pattern(pattern, path):
    # NOTE: '/' is translated to ^ for .gitignore style patterns,
    # it is natural to proceed absolute path with this sign
    if pattern.startswith("//"):
        return pattern

    negation = False

    if pattern.startswith("!"):
        pattern = pattern[1:]
        negation = True

    if pattern.startswith("/"):
        pattern = os.path.normpath(path) + pattern
    else:
        pattern = os.path.join(path, pattern)

    pattern = os.path.join(path, pattern)
    pattern = "/" + pattern

    if negation:
        pattern = "!" + pattern

    return pattern


class DvcIgnorePatterns(DvcIgnore):
    def __init__(self, ignore_file_path, patterns):
        self.ignore_file_path = ignore_file_path
        self.dirname = os.path.normpath(os.path.dirname(ignore_file_path))

        patterns = [full_path_pattern(p, self.dirname) for p in patterns]
        self.spec = PathSpec.from_lines(GitWildMatchPattern, patterns)

    def __call__(self, root, dirs, files):
        files = [f for f in files if not self.matches(root, f)]
        dirs = [d for d in dirs if not self.matches(root, d)]

        return dirs, files

    def matches(self, dirname, basename):
        abs_path = os.path.join(dirname, basename)

        result = self.spec.match_file(abs_path)

        return result

    def __hash__(self):
        return hash(self.ignore_file_path)


class DvcIgnoreDirs(DvcIgnore):
    def __init__(self, basenames):
        self.basenames = set(basenames)

    def __call__(self, root, dirs, files):
        dirs = [d for d in dirs if d not in self.basenames]

        return dirs, files


class DvcIgnoreFile(DvcIgnore):
    def __init__(self, basename):
        self.basename = basename

    def __call__(self, root, dirs, files):
        files = [f for f in files if not f == self.basename]

        return dirs, files


class DvcIgnoreFilter(object):
    def __init__(self):
        self.ignores = [
            DvcIgnoreDirs([".git", ".hg", ".dvc"]),
            DvcIgnoreFile(".dvcignore"),
        ]

    def update(self, ignore_file_path, patterns):
        self.ignores.append(DvcIgnorePatterns(ignore_file_path, patterns))

    def __call__(self, root, dirs, files):
        for ignore in self.ignores:
            dirs, files = ignore(root, dirs, files)

        return dirs, files
