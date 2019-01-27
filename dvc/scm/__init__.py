"""Manages source control systems(e.g. Git)."""

from __future__ import unicode_literals

from dvc.scm.base import Base
from dvc.scm.git import Git


def SCM(root_dir, project=None):  # pylint: disable=invalid-name
    """Returns SCM instance that corresponds to a project at the specified
    path.

    Args:
        root_dir (str): path to a root directory of the project.
        project (dvc.project.Project): dvc project instance that root_dir
            belongs to.

    Returns:
        dvc.scm.base.Base: SCM instance.
    """
    if Git.is_repo(root_dir) or Git.is_submodule(root_dir):
        return Git(root_dir, project=project)

    return Base(root_dir, project=project)
