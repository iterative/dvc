import logging
import os
from typing import TYPE_CHECKING, Dict, Iterable, Mapping, Optional, Type

from dvc.types import StrPath

from .backend.base import BaseExecutorBackend
from .backend.terraform import TerraformBackend

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)

BackendCls = Type[BaseExecutorBackend]


class ExecutorBackends(Mapping):
    DEFAULT: Dict[str, BackendCls] = {
        "terraform": TerraformBackend,
    }

    def __getitem__(self, key: str) -> BaseExecutorBackend:
        """Lazily initialize backends and cache it afterwards"""
        initialized = self.initialized.get(key)
        if not initialized:
            backend = self.backends[key]
            initialized = backend(
                os.path.join(self.tmp_dir, key), **self.kwargs
            )
            self.initialized[key] = initialized
        return initialized

    def __init__(
        self,
        selected: Optional[Iterable[str]],
        tmp_dir: StrPath,
        **kwargs,
    ) -> None:
        selected = selected or list(self.DEFAULT)
        self.backends = {key: self.DEFAULT[key] for key in selected}

        self.initialized: Dict[str, BaseExecutorBackend] = {}

        self.tmp_dir = tmp_dir
        self.kwargs = kwargs

    def __iter__(self):
        return iter(self.backends)

    def __len__(self) -> int:
        return len(self.backends)

    def close_initialized(self) -> None:
        for backend in self.initialized.values():
            backend.close()


class ExecutorManager:
    """Class that manages dvc executors.

    Args:
        repo (dvc.repo.Repo): repo instance that belongs to the repo that
            we are working on.

    Raises:
        config.ConfigError: thrown when config has invalid format.
    """

    CLOUD_BACKENDS = {
        "aws": "terraform",
        "azure": "terraform",
    }

    def __init__(
        self, repo: "Repo", backends: Optional[Iterable[str]] = None, **kwargs
    ):
        self.repo = repo
        tmp_dir = os.path.join(self.repo.tmp_dir, "exec")
        self.backends = ExecutorBackends(backends, tmp_dir=tmp_dir, **kwargs)

    def get_executor_config(
        self,
        name: Optional[str] = None,
    ):
        from dvc.config import NoExecutorError

        if not name:
            name = self.repo.config["core"].get("executor")

        if name:
            return self._get_config(name=name)

        if bool(self.repo.config["executor"]):
            error_msg = (
                "no executor specified. Setup default executor with\n"
                "    dvc executor default <name>\n"
            )
        else:
            error_msg = (
                "no executor specified. Create a default executor with\n"
                "    dvc executor add -d <name> <cloud>"
            )

        raise NoExecutorError(error_msg)

    def _get_config(self, **kwargs):
        config = self.repo.config
        name = kwargs.get("name")
        if name:
            try:
                conf = config["executor"][name.lower()]
            except KeyError:
                from dvc.config import ExecutorNotFoundError

                raise ExecutorNotFoundError(f"executor '{name}' doesn't exist")
        else:
            conf = kwargs
        return conf

    def _get_backend(self, cloud: str) -> BaseExecutorBackend:
        from dvc.config import NoExecutorError

        try:
            backend = self.CLOUD_BACKENDS[cloud]
            return self.backends[backend]
        except KeyError:
            raise NoExecutorError(f"Executor platform '{cloud}' unsupported")
