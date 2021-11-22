"""Manages source control systems (e.g. Git)."""

from dvc.scm.base import Base, NoSCMError
from dvc.scm.git import Git


# Syntactic sugar to signal that this is an actual implementation for a DVC
# project under no SCM control.
class NoSCM(Base):
    def __getattr__(self, name):
        raise NoSCMError


def SCM(
    root_dir, search_parent_directories=True, no_scm=False
):  # pylint: disable=invalid-name
    """Returns SCM instance that corresponds to a repo at the specified
    path.

    Args:
        root_dir (str): path to a root directory of the repo.
        search_parent_directories (bool): whether to look for repo root in
        parent directories.
        no_scm (bool): return NoSCM if True.

    Returns:
        dvc.scm.base.Base: SCM instance.
    """
    from dvc.scm.base import SCMError
    from dvc.scm.exceptions import SCMError as InternalSCMError

    try:
        if no_scm:
            return NoSCM(root_dir)
        return Git(
            root_dir, search_parent_directories=search_parent_directories
        )
    except InternalSCMError as exc:
        raise SCMError(str(exc))


def clone(url: str, to_path: str, **kwargs):
    from .base import CloneError
    from .exceptions import CloneError as InternalCloneError

    try:
        return Git.clone(url, to_path, **kwargs)
    except InternalCloneError as exc:
        raise CloneError(str(exc))


def resolve_rev(scm: "Git", rev: str) -> str:
    from .base import RevError
    from .exceptions import RevError as InternalRevError

    try:
        return scm.resolve_rev(rev)
    except InternalRevError as exc:
        raise RevError(str(exc))
