import logging
from abc import ABC, abstractmethod
from typing import Iterable, Optional

from dvc.types import StrPath
from dvc.utils.fs import makedirs

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
    ) -> Iterable[dict]:
        """Iterate over status of all instances of the specified machine."""

    def close(self):
        pass
