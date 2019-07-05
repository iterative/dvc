from __future__ import unicode_literals

import logging
import os
from pathspec import PathSpec
from pathspec.patterns import GitWildMatchPattern

from dvc.utils import relpath
from dvc.utils.fs import get_parent_dirs_up_to

from dvc.utils.compat import open

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
    def __init__(self, ignore_file_path):
        self.ignore_file_path = ignore_file_path
        self.dirname = os.path.normpath(os.path.dirname(ignore_file_path))

        with open(ignore_file_path, encoding="utf-8") as fobj:
            self.rel_spec = PathSpec.from_lines(GitWildMatchPattern, fobj)

    def __call__(self, root, dirs, files):
        files = [f for f in files if not self.matches(root, f)]
        dirs = [d for d in dirs if not self.matches(root, d)]

        return dirs, files

    def matches(self, dirname, basename):
        abs_path = os.path.join(dirname, basename)
        rel_path = relpath(abs_path, self.dirname)

        if os.pardir + os.sep in rel_path:
            return False
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
    def __init__(self, root=None):
        self.ignores = [DvcIgnoreDirs([".git", ".hg", ".dvc"])]
        self.root = root

    def load_upper_levels(self, top):
        top = os.path.abspath(top)
        if self.root:
            parent_dirs = get_parent_dirs_up_to(top, self.root)
            parent_dirs.append(top)
            for d in parent_dirs:
                self.update(d)

    def update(self, dirname):
        ignore_file_path = os.path.join(dirname, DvcIgnore.DVCIGNORE_FILE)
        if os.path.exists(ignore_file_path):
            self.ignores.append(DvcIgnorePatterns(ignore_file_path))

    def __call__(self, root, dirs, files):
        for ignore in self.ignores:
            dirs, files = ignore(root, dirs, files)

        return dirs, files
