"""Manages source control systems(e.g. Git) in dvc."""

from __future__ import unicode_literals

import os

from dvc.exceptions import DvcException


class SCMError(DvcException):
    """Base class for source control management errors."""


class FileNotInRepoError(DvcException):
    """Thrown when trying to find .gitignore for a file that is not in a scm
    repository.
    """


class FileNotInCommitError(DvcException):
    """Thrown when trying to find a file/directory that is not
    in the specified commit in the repository.
    """


class FileNotInTargetSubdirError(DvcException):
    """Thrown when trying to place .gitignore for a file that not in
    the file subdirectory."""


class Base(object):
    """Base class for source control management driver implementations."""

    def __init__(self, root_dir=os.curdir, repo=None):
        self.repo = repo
        self.root_dir = os.path.realpath(root_dir)

    def __repr__(self):
        return "{class_name}: '{directory}'".format(
            class_name=type(self).__name__, directory=self.dir
        )

    @property
    def dir(self):
        """Path to a directory with SCM specific information."""
        return None

    @staticmethod
    def is_repo(root_dir):  # pylint: disable=unused-argument
        """Returns whether or not root_dir is a valid SCM repository."""
        return True

    @staticmethod
    def is_submodule(root_dir):  # pylint: disable=unused-argument
        """Returns whether or not root_dir is a valid SCM repository
        submodule.
        """
        return True

    def ignore(self, path):  # pylint: disable=unused-argument
        """Makes SCM ignore a specified path."""

    def ignore_remove(self, path):  # pylint: disable=unused-argument
        """Makes SCM stop ignoring a specified path."""

    @property
    def ignore_file(self):
        """Filename for a file that contains ignored paths for this SCM."""

    def ignore_list(self, p_list):
        """Makes SCM ignore all paths specified in a list."""
        return [self.ignore(path) for path in p_list]

    def add(self, paths):
        """Makes SCM start tracking every path from a specified list of paths.
        """

    def commit(self, msg):
        """Makes SCM create a commit."""

    def checkout(self, branch, create_new=False):
        """Makes SCM checkout a branch."""

    def branch(self, branch):
        """Makes SCM create a branch with a specified name."""

    def tag(self, tag):
        """Makes SCM create a tag with a specified name."""

    def untracked_files(self):  # pylint: disable=no-self-use
        """Returns a list of untracked files."""
        return []

    def is_tracked(self, path):  # pylint: disable=no-self-use, unused-argument
        """Returns whether or not a specified path is tracked."""
        return False

    def is_dirty(self):
        """Return whether the SCM contains uncommited changes."""
        return False

    def active_branch(self):  # pylint: disable=no-self-use
        """Returns current branch in the repo."""
        return ""

    def list_branches(self):  # pylint: disable=no-self-use
        """Returns a list of available branches in the repo."""
        return []

    def list_tags(self):  # pylint: disable=no-self-use
        """Returns a list of available tags in the repo."""
        return []

    def install(self):
        """Adds dvc commands to SCM hooks for the repo."""

    def cleanup_ignores(self):
        """
        This method should clean up ignores (eg. entries in .gitignore),
        use, when method editing ignores (eg. add, run, import) fails to
        perform its task.
        """

    def reset_ignores(self):
        """
        Method to reset in-memory ignore storing mechanism.
        """

    def remind_to_track(self):
        """
        Method to remind user to track newly created files handled by scm
        """

    def track_file(self, path):
        """
        Method to add file to mechanism that will remind user
        to track new files
        """

    def belongs_to_scm(self, path):
        """Return boolean whether file belongs to scm"""
