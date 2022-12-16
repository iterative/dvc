import logging
import os
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Tuple,
    Type,
)

from dvc.exceptions import DvcException
from dvc.types import StrPath

if TYPE_CHECKING:
    from dvc.repo import Repo

    from .backend.base import BaseMachineBackend

    BackendCls = Type[BaseMachineBackend]

logger = logging.getLogger(__name__)


RESERVED_NAMES = {"local", "localhost"}

DEFAULT_STARTUP_SCRIPT = """#!/bin/bash
sudo apt-get update
sudo apt-get install --yes python3 python3-pip
python3 -m pip install --upgrade pip

pushd /etc/apt/sources.list.d
sudo wget https://dvc.org/deb/dvc.list
sudo apt-get update
sudo apt-get install --yes dvc
popd

sudo echo "OK" > /var/log/dvc-machine-init.log
"""


def validate_name(name: str):
    from dvc.exceptions import InvalidArgumentError

    name = name.lower()
    if name in RESERVED_NAMES:
        raise InvalidArgumentError(
            f"Machine name '{name}' is reserved for internal DVC use."
        )


class MachineBackends(Mapping):
    try:
        from .backend.terraform import TerraformBackend
    except ImportError:
        TerraformBackend = None  # type: ignore[assignment, misc]

    DEFAULT: Dict[str, Optional["BackendCls"]] = {
        "terraform": TerraformBackend,
    }

    def __getitem__(self, key: str) -> "BaseMachineBackend":
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
        self.backends: Dict[str, "BackendCls"] = {}
        for key in selected:
            cls = self.DEFAULT.get(key)
            if cls is None:
                raise DvcException(
                    f"'dvc machine' backend '{key}' is missing required "
                    "dependencies. Install them with:\n"
                    f"\tpip install dvc[{key}]"
                )
            self.backends[key] = cls

        self.initialized: Dict[str, "BaseMachineBackend"] = {}

        self.tmp_dir = tmp_dir
        self.kwargs = kwargs

    def __iter__(self):
        return iter(self.backends)

    def __len__(self) -> int:
        return len(self.backends)

    def close_initialized(self) -> None:
        for backend in self.initialized.values():
            backend.close()


class MachineManager:
    """Class that manages dvc cloud machines.

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
        tmp_dir = os.path.join(self.repo.tmp_dir, "machine")
        self.backends = MachineBackends(backends, tmp_dir=tmp_dir, **kwargs)

    def get_config_and_backend(
        self,
        name: Optional[str] = None,
    ) -> Tuple[dict, "BaseMachineBackend"]:
        from dvc.config import NoMachineError

        if not name:
            name = self.repo.config["core"].get("machine")

        if name:
            config = self._get_config(name=name)
            backend = self._get_backend(config["cloud"])
            return config, backend

        if bool(self.repo.config["machine"]):
            error_msg = (
                "no machine specified. Setup default machine with\n"
                "    dvc machine default <name>\n"
            )
        else:
            error_msg = (
                "no machine specified. Create a default machine with\n"
                "    dvc machine add -d <name> <cloud>"
            )

        raise NoMachineError(error_msg)

    def _get_config(self, **kwargs):
        config = self.repo.config
        name = kwargs.get("name")
        if name:
            try:
                conf = config["machine"][name.lower()]
                conf["name"] = name
            except KeyError:
                from dvc.config import MachineNotFoundError

                raise MachineNotFoundError(f"machine '{name}' doesn't exist")
        else:
            conf = kwargs
        return conf

    def _get_backend(self, cloud: str) -> "BaseMachineBackend":
        from dvc.config import NoMachineError

        try:
            backend = self.CLOUD_BACKENDS[cloud]
            return self.backends[backend]
        except KeyError:
            raise NoMachineError(f"Machine platform '{cloud}' unsupported")

    def create(self, name: Optional[str]):
        """Create and start the specified machine instance."""
        config, backend = self.get_config_and_backend(name)
        if "startup_script" in config:
            with open(config["startup_script"], encoding="utf-8") as fobj:
                startup_script = fobj.read()
        else:
            startup_script = DEFAULT_STARTUP_SCRIPT
        config["startup_script"] = startup_script
        config.pop("setup_script", None)
        return backend.create(**config)

    def destroy(self, name: Optional[str]):
        """Destroy the specified machine instance."""
        config, backend = self.get_config_and_backend(name)
        return backend.destroy(**config)

    def get_sshfs(self, name: Optional[str]):
        config, backend = self.get_config_and_backend(name)
        return backend.get_sshfs(**config)

    def run_shell(self, name: Optional[str]):
        config, backend = self.get_config_and_backend(name)
        return backend.run_shell(**config)

    def status(self, name: str) -> Iterator[Dict[Any, Any]]:
        config, backend = self.get_config_and_backend(name)
        return backend.instances(**config)

    def rename(self, name: str, new: str):
        """move configuration to a new location if the machine is running."""
        config, backend = self.get_config_and_backend(name)
        return backend.rename(new=new, **config)

    def get_executor_kwargs(self, name: Optional[str]):
        config, backend = self.get_config_and_backend(name)
        return backend.get_executor_kwargs(**config)

    def get_setup_script(self, name: Optional[str]) -> Optional[str]:
        config, _backend = self.get_config_and_backend(name)
        return config.get("setup_script")
