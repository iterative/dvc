import os
from contextlib import contextmanager
from functools import partialmethod
from typing import TYPE_CHECKING, Iterator, Optional

from dvc.exceptions import DvcException
from dvc.fs.ssh import SSHFileSystem
from dvc.utils.fs import makedirs

from .base import BaseMachineBackend

if TYPE_CHECKING:
    from dvc.repo.experiments.executor.base import BaseExecutor
    from dvc.types import StrPath


class TerraformBackend(BaseMachineBackend):
    def __init__(self, tmp_dir: "StrPath", **kwargs):
        super().__init__(tmp_dir, **kwargs)
        makedirs(tmp_dir, exist_ok=True)

    @contextmanager
    def make_tpi(self, name: str):
        from tpi import TPIError
        from tpi.terraform import TerraformBackend as TPIBackend

        try:
            working_dir = os.path.join(self.tmp_dir, name)
            makedirs(working_dir, exist_ok=True)
            yield TPIBackend(working_dir=working_dir)
        except TPIError as exc:
            raise DvcException("TPI operation failed") from exc

    def _tpi_func(self, fname, name: Optional[str] = None, **config):
        from tpi import TPIError

        assert name
        with self.make_tpi(name) as tpi:
            func = getattr(tpi, fname)
            try:
                return func(name=name, **config)
            except TPIError as exc:
                raise DvcException(f"TPI {fname} failed") from exc

    create = partialmethod(_tpi_func, "create")  # type: ignore[assignment]
    destroy = partialmethod(_tpi_func, "destroy")  # type: ignore[assignment]
    instances = partialmethod(
        _tpi_func, "instances"
    )  # type: ignore[assignment]
    run_shell = partialmethod(
        _tpi_func, "run_shell"
    )  # type: ignore[assignment]

    def get_executor(
        self, name: Optional[str] = None, **config
    ) -> "BaseExecutor":
        raise NotImplementedError

    @contextmanager
    def get_sshfs(  # pylint: disable=unused-argument
        self, name: Optional[str] = None, **config
    ) -> Iterator["SSHFileSystem"]:
        from tpi import TerraformProviderIterative

        assert name
        with self.make_tpi(name) as tpi:
            resource = tpi.default_resource(name)
        with TerraformProviderIterative.pemfile(resource) as pem:
            fs = SSHFileSystem(
                host=resource["instance_ip"],
                user="ubuntu",
                keyfile=pem,
            )
            yield fs

    def rename(self, name: str, new: str, **config):
        """rename a dvc machine instance to a new name"""
        import os
        import shutil

        from tpi import TPIError

        from dvc.exceptions import DvcException

        mtype = "iterative_machine"

        assert name and new

        new_dir = os.path.join(self.tmp_dir, new)
        old_dir = os.path.join(self.tmp_dir, name)
        if os.path.exists(new_dir):
            raise DvcException(f"rename failed: path {new_dir} already exists")

        if not os.path.exists(old_dir):
            return

        try:
            self.state_mv(f"{mtype}.{name}", f"{mtype}.{new}", **config)
        except TPIError as exc:
            raise DvcException(f"rename failed: {exc}") from exc

        shutil.move(old_dir, new_dir)
