"""Manages source control systems(e.g. Git)."""

from __future__ import unicode_literals

from dvc.scm.base import Base
from dvc.scm.git import Git

import dvc


# just a sugar to point that this is an actual implementation for a dvc
# project under no SCM control
class NoSCM(Base):
    pass


def SCM(root_dir, repo=None):  # pylint: disable=invalid-name
    """Returns SCM instance that corresponds to a repo at the specified
    path.

    Args:
        root_dir (str): path to a root directory of the repo.
        repo (dvc.repo.Repo): dvc repo instance that root_dir belongs to.

    Returns:
        dvc.scm.base.Base: SCM instance.
    """
    if Git.is_repo(root_dir) or Git.is_submodule(root_dir):
        return Git(root_dir, repo=repo)

    return NoSCM(root_dir, repo=repo)


def scm_context(method):
    def run(*args, **kw):
        repo = args[0]
        assert isinstance(repo, dvc.repo.Repo)
        try:
            result = method(*args, **kw)
            repo.scm.reset_ignores()
            repo.scm.remind_to_track()
            return result
        except Exception as e:
            repo.scm.cleanup_ignores()
            raise e

    return run
