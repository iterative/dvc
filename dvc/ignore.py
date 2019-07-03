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


class DvcIgnoreFromFile(DvcIgnore):
    def __init__(self, ignore_file_path, tree):
        self.ignore_file_path = ignore_file_path
        self.dirname = os.path.normpath(os.path.dirname(ignore_file_path))

        # TODO maybe we should readlines, join with ignore_file_path.dirname
        # and then parse?
        with tree.open(ignore_file_path) as fobj:
            ignore_lines = fobj.readlines()

        def full_path_pattern(pattern):
            negation = False
            if not pattern.startswith("/"):
                if pattern.startswith("!"):
                    pattern = pattern[1:]
                    negation = True
                pattern = os.path.join(self.dirname, pattern)
            # TODO NOTE: need to escape beggining of path
            pattern = "/" + pattern
            if negation:
                pattern = "!" + pattern
            return pattern

        ignore_lines = [full_path_pattern(p) for p in ignore_lines]
        self.ignore_spec = PathSpec.from_lines(
            GitWildMatchPattern, ignore_lines
        )

    def __call__(self, root, dirs, files):
        files = [f for f in files if not self.matches(root, f)]
        dirs = [d for d in dirs if not self.matches(root, d)]

        return dirs, files

    def matches(self, dirname, basename):
        abs_path = os.path.abspath(os.path.join(dirname, basename))
        # relative_path = relpath(abs_path, self.dirname)
        result = self.ignore_spec.match_file(abs_path)
        return result

    def __hash__(self):
        return hash(self.ignore_file_path)


class DvcIgnoreConstant(DvcIgnore):
    def __init__(self, basename):
        self.basename = basename


class DvcIgnoreDir(DvcIgnoreConstant):
    def __call__(self, root, dirs, files):
        dirs = [d for d in dirs if not d == self.basename]

        return dirs, files


class DvcIgnoreFile(DvcIgnoreConstant):
    def __call__(self, root, dirs, files):
        files = [f for f in files if not f == self.basename]

        return dirs, files


class DvcIgnoreFilter(object):
    def __init__(self, tree):
        self.ignores = [
            DvcIgnoreDir(".git"),
            DvcIgnoreDir(".hg"),
            DvcIgnoreDir(".dvc"),
            DvcIgnoreFile(".dvcignore"),
        ]

        self.tree = tree

    def update(self, wdir):
        ignore_file_path = os.path.join(wdir, DvcIgnore.DVCIGNORE_FILE)
        if self.tree.exists(ignore_file_path):
            file_ignore = DvcIgnoreFromFile(ignore_file_path, tree=self.tree)
            self.ignores.append(file_ignore)

    def __call__(self, root, dirs, files):
        for ignore in self.ignores:
            dirs, files = ignore(root, dirs, files)

        return dirs, files
