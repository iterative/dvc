import logging
from abc import ABC, abstractmethod
from typing import Iterable

from dvc.types import StrPath
from dvc.utils.fs import makedirs

logger = logging.getLogger(__name__)


class BaseExecutorBackend(ABC):
    def __init__(self, tmp_dir: StrPath, **kwargs):
        self.tmp_dir = tmp_dir
        makedirs(self.tmp_dir, exist_ok=True)

    @abstractmethod
    def init(self, **config):
        """Initialize an instance of the specified executor."""

    @abstractmethod
    def destroy(self, **config):
        """Destroy all instances of the specified executor."""

    @abstractmethod
    def instances(self, **config) -> Iterable[dict]:
        """Iterate over status of all instances of the specified executor."""

    def close(self):
        pass
