from __future__ import unicode_literals

import os
from pathspec import PathSpec
from pathspec.patterns import GitWildMatchPattern

from dvc.utils import relpath
from dvc.utils.fs import get_parent_dirs_up_to


class DvcIgnoreFileHandler(object):
    def __init__(self, tree):
        self.tree = tree

    def read_patterns(self, path):
        with self.tree.open(path) as fobj:
            return PathSpec.from_lines(GitWildMatchPattern, fobj)

    def get_repo_root(self):
        return self.tree.tree_root


class DvcIgnore(object):
    DVCIGNORE_FILE = ".dvcignore"

    def __call__(self, root, dirs, files):
        raise NotImplementedError


class DvcIgnoreFromFile(DvcIgnore):
    def __init__(self, ignore_file_path, ignore_handler):
        self.ignore_file_path = ignore_file_path
        self.dirname = os.path.normpath(os.path.dirname(ignore_file_path))

        self.ignore_spec = ignore_handler.read_patterns(ignore_file_path)

    def __call__(self, root, dirs, files):
        files = [f for f in files if not self.matches(root, f)]
        dirs = [d for d in dirs if not self.matches(root, d)]

        return dirs, files

    def matches(self, dirname, basename):
        abs_path = os.path.join(dirname, basename)
        relative_path = relpath(abs_path, self.dirname)
        if os.name == "nt":
            relative_path = relative_path.replace("\\", "/")

        return self.ignore_spec.match_file(relative_path)

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
    def __init__(self, wdir, ignore_file_handler=None):
        self.ignores = [
            DvcIgnoreDir(".git"),
            DvcIgnoreDir(".hg"),
            DvcIgnoreDir(".dvc"),
            DvcIgnoreFile(".dvcignore"),
        ]

        self.ignore_file_handler = ignore_file_handler
        self._process_ignores_in_parent_dirs(wdir)

    def _process_ignores_in_parent_dirs(self, wdir):
        if self.ignore_file_handler:
            wdir = os.path.normpath(os.path.abspath(wdir))
            ignore_search_end_dir = self.ignore_file_handler.get_repo_root()
            parent_dirs = get_parent_dirs_up_to(wdir, ignore_search_end_dir)
            for d in parent_dirs:
                self.update(d)

    def update(self, wdir):
        ignore_file_path = os.path.join(wdir, DvcIgnore.DVCIGNORE_FILE)
        if os.path.exists(ignore_file_path):
            file_ignore = DvcIgnoreFromFile(
                ignore_file_path, ignore_handler=self.ignore_file_handler
            )
            self.ignores.append(file_ignore)

    def __call__(self, root, dirs, files):
        if self.ignore_file_handler:
            self.update(root)

        for ignore in self.ignores:
            dirs, files = ignore(root, dirs, files)

        return dirs, files
