from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterator, Optional

from tpi import TerraformProviderIterative, terraform

from dvc.fs.ssh import SSHFileSystem

from .base import BaseMachineBackend

if TYPE_CHECKING:
    from dvc.repo.experiments.executor.base import BaseExecutor
    from dvc.types import StrPath


class TerraformBackend(terraform.TerraformBackend, BaseMachineBackend):
    def __init__(self, tmp_dir: "StrPath", **kwargs):
        super().__init__(tmp_dir)

    def get_executor(
        self, name: Optional[str] = None, **config
    ) -> "BaseExecutor":
        raise NotImplementedError

    @contextmanager
    def get_sshfs(  # pylint: disable=unused-argument
        self, name: Optional[str] = None, **config
    ) -> Iterator["SSHFileSystem"]:
        resource = self._default_resource(name)
        with TerraformProviderIterative.pemfile(resource) as pem:
            fs = SSHFileSystem(
                host=resource["instance_ip"],
                user="ubuntu",
                keyfile=pem,
            )
            yield fs
