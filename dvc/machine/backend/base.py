from abc import abstractmethod
from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterator, Optional

from tpi import base

if TYPE_CHECKING:
    from dvc.fs.ssh import SSHFileSystem
    from dvc.repo.experiments.executor.base import BaseExecutor


class BaseMachineBackend(base.BaseMachineBackend):
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
