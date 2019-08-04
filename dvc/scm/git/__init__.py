"""Manages Git."""

from __future__ import unicode_literals

import os
import logging

from dvc.utils.compat import str, open
from dvc.utils import fix_env, relpath
from dvc.scm.base import (
    Base,
    SCMError,
    FileNotInRepoError,
    FileNotInTargetSubdirError,
)
from dvc.scm.git.tree import GitTree


logger = logging.getLogger(__name__)


DIFF_A_TREE = "a_tree"
DIFF_B_TREE = "b_tree"
DIFF_A_REF = "a_ref"
DIFF_B_REF = "b_ref"
DIFF_EQUAL = "equal"


class Git(Base):
    """Class for managing Git."""

    GITIGNORE = ".gitignore"
    GIT_DIR = ".git"

    def __init__(self, root_dir=os.curdir, repo=None):
        """Git class constructor.
        Requires `Repo` class from `git` module (from gitpython package).
        """
        super(Git, self).__init__(root_dir, repo=repo)

        import git
        from git.exc import InvalidGitRepositoryError

        try:
            self.repo = git.Repo(self.root_dir)
        except InvalidGitRepositoryError:
            msg = "{} is not a git repository"
            raise SCMError(msg.format(self.root_dir))

        # NOTE: fixing LD_LIBRARY_PATH for binary built by PyInstaller.
        # http://pyinstaller.readthedocs.io/en/stable/runtime-information.html
        env = fix_env(None)
        libpath = env.get("LD_LIBRARY_PATH", None)
        self.repo.git.update_environment(LD_LIBRARY_PATH=libpath)

        self.ignored_paths = []
        self.files_to_track = set()

    @staticmethod
    def is_repo(root_dir):
        return os.path.isdir(Git._get_git_dir(root_dir))

    @staticmethod
    def is_submodule(root_dir):
        return os.path.isfile(Git._get_git_dir(root_dir))

    @staticmethod
    def _get_git_dir(root_dir):
        return os.path.join(root_dir, Git.GIT_DIR)

    @property
    def dir(self):
        return self.repo.git_dir

    @property
    def ignore_file(self):
        return self.GITIGNORE

    def _get_gitignore(self, path, ignore_file_dir=None):
        if not ignore_file_dir:
            ignore_file_dir = os.path.dirname(os.path.realpath(path))

        assert os.path.isabs(path)
        assert os.path.isabs(ignore_file_dir)

        if not path.startswith(ignore_file_dir):
            msg = (
                "{} file has to be located in one of '{}' subdirectories"
                ", not outside '{}'"
            )
            raise FileNotInTargetSubdirError(
                msg.format(self.GITIGNORE, path, ignore_file_dir)
            )

        entry = relpath(path, ignore_file_dir).replace(os.sep, "/")
        # NOTE: using '/' prefix to make path unambiguous
        if len(entry) > 0 and entry[0] != "/":
            entry = "/" + entry

        gitignore = os.path.join(ignore_file_dir, self.GITIGNORE)

        if not gitignore.startswith(os.path.realpath(self.root_dir)):
            raise FileNotInRepoError(path)

        return entry, gitignore

    @staticmethod
    def _ignored(entry, gitignore_path):
        if os.path.exists(gitignore_path):
            with open(gitignore_path, "r") as fobj:
                ignore_list = fobj.readlines()
            return any(
                filter(lambda x: x.strip() == entry.strip(), ignore_list)
            )
        return False

    def ignore(self, path, in_curr_dir=False):
        base_dir = (
            os.path.realpath(os.curdir)
            if in_curr_dir
            else os.path.dirname(path)
        )
        entry, gitignore = self._get_gitignore(path, base_dir)

        if self._ignored(entry, gitignore):
            return

        msg = "Adding '{}' to '{}'.".format(relpath(path), relpath(gitignore))
        logger.info(msg)

        self._add_entry_to_gitignore(entry, gitignore)

        self.track_file(relpath(gitignore))

        self.ignored_paths.append(path)

    @staticmethod
    def _add_entry_to_gitignore(entry, gitignore):
        with open(gitignore, "a+", encoding="utf-8") as fobj:
            fobj.seek(0, os.SEEK_END)
            if fobj.tell() == 0:
                # Empty file
                prefix = ""
            else:
                fobj.seek(fobj.tell() - 1, os.SEEK_SET)
                last = fobj.read(1)
                prefix = "" if last == "\n" else "\n"
            fobj.write("{}{}\n".format(prefix, entry))

    def ignore_remove(self, path):
        entry, gitignore = self._get_gitignore(path)

        if not os.path.exists(gitignore):
            return

        with open(gitignore, "r") as fobj:
            lines = fobj.readlines()

        filtered = list(filter(lambda x: x.strip() != entry.strip(), lines))

        with open(gitignore, "w") as fobj:
            fobj.writelines(filtered)

        self.track_file(relpath(gitignore))

    def add(self, paths):
        # NOTE: GitPython is not currently able to handle index version >= 3.
        # See https://github.com/iterative/dvc/issues/610 for more details.
        try:
            self.repo.index.add(paths)
        except AssertionError:
            msg = (
                "failed to add '{}' to git. You can add those files"
                " manually using 'git add'."
                " See 'https://github.com/iterative/dvc/issues/610'"
                " for more details.".format(str(paths))
            )

            logger.exception(msg)

    def commit(self, msg):
        self.repo.index.commit(msg)

    def checkout(self, branch, create_new=False):
        if create_new:
            self.repo.git.checkout("HEAD", b=branch)
        else:
            self.repo.git.checkout(branch)

    def branch(self, branch):
        self.repo.git.branch(branch)

    def tag(self, tag):
        self.repo.git.tag(tag)

    def untracked_files(self):
        files = self.repo.untracked_files
        return [os.path.join(self.repo.working_dir, fname) for fname in files]

    def is_tracked(self, path):
        # it is equivalent to `bool(self.repo.git.ls_files(path))` by
        # functionality, but ls_files fails on unicode filenames
        path = relpath(path, self.root_dir)
        return path in [i[0] for i in self.repo.index.entries]

    def is_dirty(self):
        return self.repo.is_dirty()

    def active_branch(self):
        return self.repo.active_branch.name

    def list_branches(self):
        return [h.name for h in self.repo.heads]

    def list_tags(self):
        return [t.name for t in self.repo.tags]

    def _install_hook(self, name, cmd):
        command = '[ -z "$(git ls-files .dvc)" ] || exec dvc {}'.format(cmd)

        hook = os.path.join(self.root_dir, self.GIT_DIR, "hooks", name)

        if os.path.isfile(hook):
            with open(hook, "r+") as fobj:
                if command not in fobj.read():
                    fobj.write("{command}\n".format(command=command))
        else:
            with open(hook, "w+") as fobj:
                fobj.write("#!/bin/sh\n" "{command}\n".format(command=command))

        os.chmod(hook, 0o777)

    def install(self):
        self._install_hook("post-checkout", "checkout")
        self._install_hook("pre-commit", "status")
        self._install_hook("pre-push", "push")

    def cleanup_ignores(self):
        for path in self.ignored_paths:
            self.ignore_remove(path)
        self.reset_ignores()

    def reset_ignores(self):
        self.ignored_paths = []

    def reset_tracked_files(self):
        self.files_to_track = set()

    def remind_to_track(self):
        if not self.files_to_track:
            return

        logger.info(
            "\n"
            "To track the changes with git, run:\n"
            "\n"
            "\tgit add {files}".format(files=" ".join(self.files_to_track))
        )

    def track_file(self, path):
        self.files_to_track.add(path)

    def belongs_to_scm(self, path):
        basename = os.path.basename(path)
        path_parts = os.path.normpath(path).split(os.path.sep)
        return basename == self.ignore_file or Git.GIT_DIR in path_parts

    def get_tree(self, rev):
        return GitTree(self.repo, rev)

    def _get_diff_trees(self, a_ref, b_ref):
        """Private method for getting the trees and commit hashes of 2 git
        references. Requires `gitdb` module (from gitpython package).

        Args:
            a_ref (str): git reference
            b_ref (str): second git reference. If None, uses HEAD

        Returns:
            tuple: tuple with elements: (trees, commits)
        """
        from gitdb.exc import BadObject, BadName

        trees = {DIFF_A_TREE: None, DIFF_B_TREE: None}
        commits = []
        if b_ref is None:
            b_ref = self.repo.head.commit
        try:
            a_commit = self.repo.git.rev_parse(a_ref, short=True)
            b_commit = self.repo.git.rev_parse(b_ref, short=True)
            # See https://gitpython.readthedocs.io
            # /en/2.1.11/reference.html#git.objects.base.Object.__str__
            commits.append(a_commit)
            commits.append(b_commit)
            trees[DIFF_A_TREE] = self.get_tree(commits[0])
            trees[DIFF_B_TREE] = self.get_tree(commits[1])
        except (BadName, BadObject) as e:
            raise SCMError("git problem", cause=e)
        return trees, commits

    def get_diff_trees(self, a_ref, b_ref=None):
        """Method for getting two repo trees between two git tag commits.
        Returns the dvc hash names of changed file/directory

        Args:
            a_ref (str): git reference
            b_ref (str): optional second git reference, default None

        Returns:
            dict: dictionary with keys: {a_ref, b_ref, equal}
                or {a_ref, b_ref, a_tree, b_tree}
        """
        diff_dct = {DIFF_EQUAL: False}
        trees, commits = self._get_diff_trees(a_ref, b_ref)
        diff_dct[DIFF_A_REF] = commits[0]
        diff_dct[DIFF_B_REF] = commits[1]
        if commits[0] == commits[1]:
            diff_dct[DIFF_EQUAL] = True
            return diff_dct
        diff_dct[DIFF_A_TREE] = trees[DIFF_A_TREE]
        diff_dct[DIFF_B_TREE] = trees[DIFF_B_TREE]
        return diff_dct

    def get_rev(self):
        return self.repo.git.rev_parse("HEAD")

    def close(self):
        self.repo.close()
