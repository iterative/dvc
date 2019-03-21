"""Manages Git."""

from __future__ import unicode_literals

import os

from dvc.utils.compat import str, open
from dvc.utils import fix_env
from dvc.scm.base import (
    Base,
    SCMError,
    FileNotInRepoError,
    FileNotInTargetSubdirError,
)
from dvc.scm.git.tree import GitTree
import dvc.logger as logger


class Git(Base):
    """Class for managing Git."""

    GITIGNORE = ".gitignore"
    GIT_DIR = ".git"

    def __init__(self, root_dir=os.curdir, repo=None):
        super(Git, self).__init__(root_dir, repo=repo)

        import git
        from git.exc import InvalidGitRepositoryError

        try:
            self.git = git.Repo(root_dir)
        except InvalidGitRepositoryError:
            msg = "{} is not a git repository"
            raise SCMError(msg.format(root_dir))

        # NOTE: fixing LD_LIBRARY_PATH for binary built by PyInstaller.
        # http://pyinstaller.readthedocs.io/en/stable/runtime-information.html
        env = fix_env(None)
        libpath = env.get("LD_LIBRARY_PATH", None)
        self.git.git.update_environment(LD_LIBRARY_PATH=libpath)

        self.ignored_paths = []
        self.files_to_track = []

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
        return self.git.git_dir

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

        entry = os.path.relpath(path, ignore_file_dir).replace(os.sep, "/")
        # NOTE: using '/' prefix to make path unambiguous
        if len(entry) > 0 and entry[0] != "/":
            entry = "/" + entry

        gitignore = os.path.join(ignore_file_dir, self.GITIGNORE)

        if not gitignore.startswith(os.path.realpath(self.root_dir)):
            raise FileNotInRepoError(path)

        return entry, gitignore

    def ignore(self, path, in_curr_dir=False):
        base_dir = (
            os.path.realpath(os.curdir)
            if in_curr_dir
            else os.path.dirname(path)
        )
        entry, gitignore = self._get_gitignore(path, base_dir)

        ignore_list = []
        if os.path.exists(gitignore):
            with open(gitignore, "r") as f:
                ignore_list = f.readlines()
            if any(filter(lambda x: x.strip() == entry.strip(), ignore_list)):
                return

        msg = "Adding '{}' to '{}'.".format(
            os.path.relpath(path), os.path.relpath(gitignore)
        )
        logger.info(msg)

        self._add_entry_to_gitignore(entry, gitignore, ignore_list)

        self.track_file(os.path.relpath(gitignore))

        self.ignored_paths.append(path)

    @staticmethod
    def _add_entry_to_gitignore(entry, gitignore, ignore_list):
        content = entry
        if ignore_list:
            content = "\n" + content
        with open(gitignore, "a", encoding="utf-8") as fobj:
            fobj.write(content)

    def ignore_remove(self, path):
        entry, gitignore = self._get_gitignore(path)

        if not os.path.exists(gitignore):
            return

        with open(gitignore, "r") as fobj:
            lines = fobj.readlines()

        filtered = list(filter(lambda x: x.strip() != entry.strip(), lines))

        with open(gitignore, "w") as fobj:
            fobj.writelines(filtered)

        self.track_file(os.path.relpath(gitignore))

    def add(self, paths):
        # NOTE: GitPython is not currently able to handle index version >= 3.
        # See https://github.com/iterative/dvc/issues/610 for more details.
        try:
            self.git.index.add(paths)
        except AssertionError:
            msg = (
                "failed to add '{}' to git. You can add those files"
                " manually using 'git add'."
                " See 'https://github.com/iterative/dvc/issues/610'"
                " for more details.".format(str(paths))
            )

            logger.error(msg)

    def commit(self, msg):
        self.git.index.commit(msg)

    def checkout(self, branch, create_new=False):
        if create_new:
            self.git.git.checkout("HEAD", b=branch)
        else:
            self.git.git.checkout(branch)

    def branch(self, branch):
        self.git.git.branch(branch)

    def tag(self, tag):
        self.git.git.tag(tag)

    def untracked_files(self):
        files = self.git.untracked_files
        return [os.path.join(self.git.working_dir, fname) for fname in files]

    def is_tracked(self, path):
        # it is equivalent to `bool(self.git.git.ls_files(path))` by
        # functionality, but ls_files fails on unicode filenames
        path = os.path.relpath(path, self.root_dir)
        return path in [i[0] for i in self.git.index.entries]

    def is_dirty(self):
        return self.git.is_dirty()

    def active_branch(self):
        return self.git.active_branch.name

    def list_branches(self):
        return [h.name for h in self.git.heads]

    def list_tags(self):
        return [t.name for t in self.git.tags]

    def _install_hook(self, name, cmd):
        hook = os.path.join(self.root_dir, self.GIT_DIR, "hooks", name)
        if os.path.isfile(hook):
            msg = "git hook '{}' already exists."
            raise SCMError(msg.format(os.path.relpath(hook)))
        with open(hook, "w+") as fobj:
            fobj.write("#!/bin/sh\nexec dvc {}\n".format(cmd))
        os.chmod(hook, 0o777)

    def install(self):
        self._install_hook("post-checkout", "checkout")
        self._install_hook("pre-commit", "status")

    def cleanup_ignores(self):
        for path in self.ignored_paths:
            self.ignore_remove(path)
        self.reset_ignores()

    def reset_ignores(self):
        self.ignored_paths = []

    def remind_to_track(self):
        if not self.files_to_track:
            return

        logger.info(
            "\n"
            "To track the changes with git run:\n"
            "\n"
            "\tgit add {files}".format(files=" ".join(self.files_to_track))
        )

    def track_file(self, path):
        self.files_to_track.append(path)

    def belongs_to_scm(self, path):
        basename = os.path.basename(path)
        path_parts = os.path.normpath(path).split(os.path.sep)
        return basename == self.ignore_file or Git.GIT_DIR in path_parts

    def get_tree(self, rev):
        return GitTree(self.git, rev)
