"""Manages source control systems (e.g. Git)."""
from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterator

from dvc.progress import Tqdm
from dvc.scm.base import Base, NoSCMError
from dvc.scm.git import Git

if TYPE_CHECKING:
    from dvc.scm.progress import GitProgressEvent


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


class TqdmGit(Tqdm):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("unit", "obj")
        super().__init__(*args, **kwargs)

    def update_git(self, event: "GitProgressEvent") -> None:
        phase, completed, total, message, *_ = event
        if phase:
            message = (phase + " | " + message) if message else phase
        if message:
            self.postfix["info"] = f" {message} | "
        if completed:
            self.update_to(completed, total)


def clone(url: str, to_path: str, **kwargs):
    from .base import CloneError
    from .exceptions import CloneError as InternalCloneError

    with TqdmGit(desc="Cloning") as pbar:
        try:
            return Git.clone(url, to_path, progress=pbar.update_git, **kwargs)
        except InternalCloneError as exc:
            raise CloneError(str(exc))


def resolve_rev(scm: "Git", rev: str) -> str:
    from .base import RevError
    from .exceptions import RevError as InternalRevError

    try:
        return scm.resolve_rev(rev)
    except InternalRevError as exc:
        raise RevError(str(exc))
