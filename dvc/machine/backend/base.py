import asyncio
import logging
import os
import sys
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterator, Optional

from dvc.exceptions import DvcException
from dvc.types import StrPath
from dvc.utils.fs import makedirs

if TYPE_CHECKING:
    from dvc.fs.ssh import SSHFileSystem
    from dvc.repo.experiments.executor.base import BaseExecutor

logger = logging.getLogger(__name__)


class BaseMachineBackend(ABC):
    def __init__(self, tmp_dir: StrPath, **kwargs):
        self.tmp_dir = tmp_dir
        makedirs(self.tmp_dir, exist_ok=True)

    @abstractmethod
    def create(self, name: Optional[str] = None, **config):
        """Create and start an instance of the specified machine."""

    @abstractmethod
    def destroy(self, name: Optional[str] = None, **config):
        """Stop and destroy all instances of the specified machine."""

    @abstractmethod
    def instances(
        self, name: Optional[str] = None, **config
    ) -> Iterator[dict]:
        """Iterate over status of all instances of the specified machine."""

    def close(self):
        pass

    @abstractmethod
    def get_executor(
        self, name: Optional[str] = None, **config
    ) -> "BaseExecutor":
        """Return an executor instance which can be used for DVC
        experiment/pipeline execution on the specified machine.
        """

    @abstractmethod
    @contextmanager
    def get_sshfs(
        self, name: Optional[str] = None, **config
    ) -> Iterator["SSHFileSystem"]:
        """Return an sshfs instance for the default directory on the
        specified machine."""

    @abstractmethod
    def run_shell(self, name: Optional[str] = None, **config):
        """Spawn an interactive SSH shell for the specified machine.

        Requires a valid 'ssh' client in the user's PATH.
        """

    def _shell(self, *args, **kwargs):
        """Spawn an interactive asyncssh shell session.

        Args will be passed into asyncssh.connect().
        """
        import asyncssh

        try:
            asyncio.run(self._shell_async(*args, **kwargs))
        except (OSError, asyncssh.Error) as exc:
            raise DvcException("SSH connection failed") from exc

    async def _shell_async(self, *args, **kwargs):
        import asyncssh

        async with asyncssh.connect(*args, **kwargs) as conn:
            await conn.run(
                term_type=os.environ.get("TERM", "xterm"),
                stdin=sys.stdin,
                stdout=sys.stdout,
                stderr=sys.stderr,
            )
