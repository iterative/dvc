from __future__ import unicode_literals

import logging
import os
from pathspec import PathSpec
from pathspec.patterns import GitWildMatchPattern

from dvc.utils import relpath

logger = logging.getLogger(__name__)


class DvcIgnore(object):
    DVCIGNORE_FILE = ".dvcignore"

    def __call__(self, root, dirs, files):
        raise NotImplementedError


# def full_path_pattern(pattern, path):
#     # NOTE: '/' is translated to ^ for .gitignore style patterns,
#     # it is natural to proceed absolute path with this sign
#     if pattern.startswith("//"):
#         return pattern
#
#     negation = False
#
#     if pattern.startswith("!"):
#         pattern = pattern[1:]
#         negation = True
#
#     if pattern.startswith("/"):
#         pattern = os.path.normpath(path) + pattern
#     else:
#         pattern = os.path.join(path, pattern)
#
#     pattern = os.path.join(path, pattern)
#     pattern = "/" + pattern
#
#     if negation:
#         pattern = "!" + pattern
#
#     return pattern


class DvcIgnorePatterns(DvcIgnore):
    def __init__(self, ignore_file_path, patterns):
        self.ignore_file_path = ignore_file_path
        self.dirname = os.path.normpath(os.path.dirname(ignore_file_path))

        abs_patterns = []
        rel_patterns = []
        for p in patterns:
            # NOTE: '/' is translated to ^ for .gitignore style patterns,
            # it is natural to proceed absolute path with this sign
            if p[0] == "/" and os.path.isabs(p[1:]):
                abs_patterns.append(p)
            else:
                rel_patterns.append(p)
        self.abs_spec = PathSpec.from_lines(GitWildMatchPattern, abs_patterns)
        self.rel_spec = PathSpec.from_lines(GitWildMatchPattern, rel_patterns)

    def __call__(self, root, dirs, files):
        files = [f for f in files if not self.matches(root, f)]
        dirs = [d for d in dirs if not self.matches(root, d)]

        return dirs, files

    def matches(self, dirname, basename):
        abs_path = os.path.join(dirname, basename)
        rel_path = relpath(abs_path, self.dirname)

        if os.pardir + os.sep in rel_path or os.path.isabs(rel_path):
            return self.abs_spec.match_file(abs_path)
        return self.rel_spec.match_file(rel_path)

    def __hash__(self):
        return hash(self.ignore_file_path)


class DvcIgnoreDirs(DvcIgnore):
    def __init__(self, basenames):
        self.basenames = set(basenames)

    def __call__(self, root, dirs, files):
        dirs = [d for d in dirs if d not in self.basenames]

        return dirs, files


class DvcIgnoreFilter(object):
    def __init__(self):
        self.ignores = [DvcIgnoreDirs([".git", ".hg", ".dvc"])]

    def update(self, ignore_file_path, patterns):
        self.ignores.append(DvcIgnorePatterns(ignore_file_path, patterns))

    def __call__(self, root, dirs, files):
        for ignore in self.ignores:
            dirs, files = ignore(root, dirs, files)

        return dirs, files
