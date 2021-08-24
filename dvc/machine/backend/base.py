import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterator, Optional

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
