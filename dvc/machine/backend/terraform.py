from contextlib import contextmanager
from functools import partial
from typing import TYPE_CHECKING, Iterator, Optional

from tpi import TerraformProviderIterative, terraform

from dvc.fs.ssh import SSHFileSystem

from .base import BaseMachineBackend

if TYPE_CHECKING:
    from dvc.types import StrPath


@contextmanager
def _sshfs(resource: dict):
    with TerraformProviderIterative.pemfile(resource) as pem:
        fs = SSHFileSystem(
            host=resource["instance_ip"],
            user="ubuntu",
            keyfile=pem,
        )
        yield fs


class TerraformBackend(terraform.TerraformBackend, BaseMachineBackend):
    def __init__(self, tmp_dir: "StrPath", **kwargs):
        super().__init__(tmp_dir)

    def get_executor_kwargs(
        self, name: Optional[str] = None, **config
    ) -> dict:
        resource = self._default_resource(name)
        return {
            "host": resource["instance_ip"],
            "username": "ubuntu",
            "fs_factory": partial(_sshfs, dict(resource)),
        }

    @contextmanager
    def get_sshfs(  # pylint: disable=unused-argument
        self, name: Optional[str] = None, **config
    ) -> Iterator["SSHFileSystem"]:
        resource = self._default_resource(name)
        with _sshfs(resource) as fs:
            yield fs
