import logging
import os
from contextlib import contextmanager
from typing import Iterable, Optional

from funcy import first
from python_terraform import Terraform

from dvc.exceptions import DvcException

from .base import BaseMachineBackend

logger = logging.getLogger(__name__)


class TerraformError(DvcException):
    pass


class DvcTerraform(Terraform):
    def cmd(self, *args, **kwargs):
        logger.debug(" ".join(self.generate_cmd_string(*args, **kwargs)))
        kwargs["capture_output"] = False
        ret, _stdout, _stderr = super().cmd(*args, **kwargs)
        if ret != 0:
            raise TerraformError("Cmd 'terraform {cmd}' failed")

    def iter_instances(self, name: str):
        """Iterate over active iterative_machine instances."""
        self.read_state_file()
        resources = getattr(self.tfstate, "resources", [])
        for resource in resources:
            if (
                resource.get("type") == "iterative_machine"
                and resource.get("name") == name
            ):
                yield from (
                    instance.get("attributes", {})
                    for instance in resource.get("instances", [])
                )


class TerraformBackend(BaseMachineBackend):
    @contextmanager
    def make_tf(self, name: str):
        from dvc.utils.fs import makedirs

        try:
            working_dir = os.path.join(self.tmp_dir, name)
            makedirs(working_dir, exist_ok=True)
            yield DvcTerraform(working_dir=working_dir)
        except TerraformError:
            raise
        except Exception as exc:
            raise TerraformError("terraform failed") from exc

    def init(self, name: Optional[str] = None, **config):
        from python_terraform import IsFlagged

        from dvc.tpi import render_json

        assert name and "cloud" in config
        with self.make_tf(name) as tf:
            tf_file = os.path.join(tf.working_dir, "main.tf.json")
            with open(tf_file, "w", encoding="utf-8") as fobj:
                fobj.write(render_json(name=name, **config, indent=2))
            tf.cmd("init")
            tf.cmd("apply", auto_approve=IsFlagged)

    def destroy(self, name: Optional[str] = None, **config):
        from python_terraform import IsFlagged

        assert name

        with self.make_tf(name) as tf:
            if first(tf.iter_instances(name)):
                tf.cmd("destroy", auto_approve=IsFlagged)

    def instances(
        self, name: Optional[str] = None, **config
    ) -> Iterable[dict]:
        assert name

        with self.make_tf(name) as tf:
            yield from tf.iter_instances(name)
