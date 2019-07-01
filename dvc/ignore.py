from __future__ import unicode_literals

import logging
import os
from pathspec import PathSpec
from pathspec.patterns import GitWildMatchPattern

from dvc.exceptions import NotDvcRepoError, DvcIgnoreError
from dvc.scm.tree import WorkingTree
from dvc.utils import relpath
from dvc.utils.fs import get_parent_dirs_up_to

logger = logging.getLogger(__name__)


class DvcIgnore(object):
    DVCIGNORE_FILE = ".dvcignore"

    def __call__(self, root, dirs, files):
        raise NotImplementedError


class DvcIgnoreFromFile(DvcIgnore):
    def __init__(self, ignore_file_path, tree):
        self.ignore_file_path = ignore_file_path
        self.dirname = os.path.normpath(os.path.dirname(ignore_file_path))

        with tree.open(ignore_file_path) as fobj:
            self.ignore_spec = PathSpec.from_lines(GitWildMatchPattern, fobj)

    def __call__(self, root, dirs, files):
        files = [f for f in files if not self.matches(root, f)]
        dirs = [d for d in dirs if not self.matches(root, d)]

        return dirs, files

    def matches(self, dirname, basename):
        abs_path = os.path.join(dirname, basename)
        relative_path = relpath(abs_path, self.dirname)
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
    def __init__(self, wdir, raise_on_dvcignore_below_top=False):
        self.ignores = [
            DvcIgnoreDir(".git"),
            DvcIgnoreDir(".hg"),
            DvcIgnoreDir(".dvc"),
            DvcIgnoreFile(".dvcignore"),
        ]

        from dvc.repo import Repo

        try:
            self.tree = WorkingTree(Repo.find_root(wdir))
        except NotDvcRepoError:
            logger.error(
                "Traversing directory outside of DvcRepo. "
                "ignore files will be read from '{}' "
                "downward.".format(wdir)
            )
            # TODO is it feasible now that we want to raise on external dir?
            self.tree = WorkingTree(os.path.abspath(wdir))

        self.raise_on_dvcignore_below_top = raise_on_dvcignore_below_top

        self._process_ignores_in_parent_dirs(wdir)

    def _process_ignores_in_parent_dirs(self, wdir):
        wdir = os.path.normpath(os.path.abspath(wdir))
        ignore_search_end_dir = self.tree.tree_root
        parent_dirs = get_parent_dirs_up_to(wdir, ignore_search_end_dir)

        if not self.raise_on_dvcignore_below_top:
            parent_dirs.append(wdir)

        for d in parent_dirs:
            self.update(d, False)

    def update(self, wdir, raise_on_dvcignore):
        ignore_file_path = os.path.join(wdir, DvcIgnore.DVCIGNORE_FILE)
        if self.tree.exists(ignore_file_path):

            if raise_on_dvcignore:
                raise DvcIgnoreError(
                    "Found dvcignore file in directory where it "
                    "should not be: '{}'".format(wdir)
                )

            file_ignore = DvcIgnoreFromFile(ignore_file_path, tree=self.tree)
            self.ignores.append(file_ignore)

    def __call__(self, root, dirs, files):
        self.update(root, self.raise_on_dvcignore_below_top)

        for ignore in self.ignores:
            dirs, files = ignore(root, dirs, files)

        return dirs, files
