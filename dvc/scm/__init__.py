"""Manages source control systems (e.g. Git)."""
from contextlib import contextmanager
from typing import Iterator

from dvc.scm.base import Base, NoSCMError
from dvc.scm.git import Git


# Syntactic sugar to signal that this is an actual implementation for a DVC
# project under no SCM control.
class NoSCM(Base):
    def __getattr__(self, name):
        raise NoSCMError


@contextmanager
def map_scm_exception(with_cause: bool = False) -> Iterator[None]:
    from dvc.scm.base import SCMError
    from dvc.scm.exceptions import SCMError as InternalSCMError

    try:
        yield
    except InternalSCMError as exc:
        into = SCMError(str(exc))
        if with_cause:
            raise into from exc
        raise into


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
    with map_scm_exception():
        if no_scm:
            return NoSCM(root_dir)
        return Git(
            root_dir, search_parent_directories=search_parent_directories
        )


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
