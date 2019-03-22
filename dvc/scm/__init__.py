"""Manages source control systems(e.g. Git)."""

from __future__ import unicode_literals

from dvc.scm.base import Base


# just a sugar to point that this is an actual implementation for a dvc
# project under no SCM control
class NoSCM(Base):
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


class SCMFactory(object):

    _scm_list = []

    def register(self, cls):
        if cls in self._scm_list:
            raise ValueError("{} is already registered".format(cls.__name__))
        self._scm_list.append(cls)
        return cls

    def __call__(self, root_dir, repo=None):  # pylint: disable=invalid-name
        """Returns SCM instance that corresponds to a repo at the specified
        path.

        Args:
            root_dir (str): path to a root directory of the repo.
            repo (dvc.repo.Repo): dvc repo instance that root_dir belongs to.

        Returns:
            dvc.scm.base.Base: SCM instance.
        """
        for scm in self._scm_list:
            if scm.is_repo(root_dir) or scm.is_submodule(root_dir):
                return scm(root_dir, repo=repo)
        return NoSCM(root_dir, repo=repo)


SCM = SCMFactory()


def scm_context(method):
    def run(repo, *args, **kw):
        try:
            result = method(repo, *args, **kw)
            repo.scm.reset_ignores()
            repo.scm.remind_to_track()
            return result
        except Exception as e:
            repo.scm.cleanup_ignores()
            raise e

    return run
