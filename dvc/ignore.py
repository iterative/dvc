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


class DvcIgnorePatterns(DvcIgnore):
    def __init__(self, ignore_file_path):
        self.ignore_file_path = ignore_file_path
        self.dirname = os.path.normpath(os.path.dirname(ignore_file_path))

        with open(ignore_file_path, encoding="utf-8") as fobj:
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
        return self.ignore_file_path == other.ignore_file_path


class DvcIgnoreDirs(DvcIgnore):
    def __init__(self, basenames):
        self.basenames = set(basenames)

    def __call__(self, root, dirs, files):
        dirs = [d for d in dirs if d not in self.basenames]

        return dirs, files


class DvcIgnoreFilter(object):
    def __init__(self, root=None):
        self.ignores = {DvcIgnoreDirs([".git", ".hg", ".dvc"])}
        self.root = root

    def load_upper_levels(self, top):
        top = os.path.abspath(top)
        if self.root:
            parent_dirs = get_parent_dirs_up_to(top, self.root)
            for d in parent_dirs:
                self.update(d)

    def update(self, dirname):
        ignore_file_path = os.path.join(dirname, DvcIgnore.DVCIGNORE_FILE)
        if os.path.exists(ignore_file_path):
            local_ignore = DvcIgnorePatterns(ignore_file_path)
            if local_ignore in self.ignores:
                self.ignores.remove(local_ignore)
            self.ignores.add(local_ignore)

    def __call__(self, root, dirs, files):
        for ignore in self.ignores:
            dirs, files = ignore(root, dirs, files)

        return dirs, files
