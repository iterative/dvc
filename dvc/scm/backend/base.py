import os
from typing import TYPE_CHECKING, Iterable, Optional, Union

from dvc.scm.base import SCMError

if TYPE_CHECKING:
    from dvc.path_info import PathInfo


class NoGitBackendError(SCMError):
    def __init__(self, func):
        super().__init__(f"No valid Git backend for '{func}'")


class BackendMethodMixin:
    """Backend methods which should be overridden."""

    def is_ignored(self, path):
        raise NotImplementedError

    def set_ref(
        self,
        name: str,
        new_ref: str,
        old_ref: Optional[str] = None,
        message: Optional[str] = None,
        symbolic: Optional[bool] = False,
    ):
        """Set the specified git ref.

        Optional kwargs:
            old_ref: If specified, ref will only be set if it currently equals
                old_ref. Has no effect is symbolic is True.
            message: Optional reflog message.
            symbolic: If True, ref will be set as a symbolic ref to new_ref
                rather than the dereferenced object.
        """
        raise NotImplementedError

    def get_ref(self, name, follow: Optional[bool] = True) -> Optional[str]:
        """Return the value of specified ref.

        If follow is false, symbolic refs will not be dereferenced.
        Returns None if the ref does not exist.
        """
        raise NotImplementedError

    def remove_ref(self, name: str, old_ref: Optional[str] = None):
        """Remove the specified git ref.

        If old_ref is specified, ref will only be removed if it currently
        equals old_ref.
        """
        raise NotImplementedError

    def push_refspec(self, url: str, src: Optional[str], dest: str):
        """Push refspec to a remote Git repo.

        Args:
            url: Remote repo Git URL (Note this must be a Git URL and not
                a remote name).
            src: Local refspec. If src ends with "/" it will be treated as a
                prefix, and all refs inside src will be pushed using dest
                as destination refspec prefix. If src is None, dest will be
                deleted from the remote.
            dest: Remote refspec.
        """
        raise NotImplementedError

    def fetch_refspecs(
        self, url: str, refspecs: Iterable[str], force: Optional[bool] = False
    ):
        """Fetch refspecs from a remote Git repo.

        Args:
            url: Remote repo Git URL (Note this must be a Git URL and not
                a remote name).
            refspecs: Iterable containing refspecs to fetch.
                Note that this will not match subkeys.
            force: If True, local refs will be overwritten.
        """
        raise NotImplementedError


class GitBackend(BackendMethodMixin):  # pylint:disable=abstract-method
    """Base Git backend class."""

    def __init__(
        self, root_dir: Union["PathInfo", str], **kwargs,
    ):
        self.root_dir = os.fspath(root_dir)

    def close(self):
        pass
