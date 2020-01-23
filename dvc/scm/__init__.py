"""Manages source control systems(e.g. Git)."""

from dvc.scm.base import Base
from dvc.scm.git import Git


# just a sugar to point that this is an actual implementation for a dvc
# project under no SCM control
class NoSCM(Base):
    pass


def SCM(root_dir):  # pylint: disable=invalid-name
    """Returns SCM instance that corresponds to a repo at the specified
    path.

    Args:
        root_dir (str): path to a root directory of the repo.
        repo (dvc.repo.Repo): DVC repo instance that root_dir belongs to.

    Returns:
        dvc.scm.base.Base: SCM instance.
    """
    if Git.is_repo(root_dir) or Git.is_submodule(root_dir):
        return Git(root_dir)

    return NoSCM(root_dir)
