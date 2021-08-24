import logging
import os
from typing import Iterable

from dvc.exceptions import DvcException
from dvc.types import StrPath

from .base import BaseMachineBackend

logger = logging.getLogger(__name__)


class TerraformError(DvcException):
    pass


class TerraformBackend(BaseMachineBackend):
    def __init__(self, tmp_dir: StrPath, **kwargs):
        from python_terraform import Terraform

        super().__init__(tmp_dir, **kwargs)
        self.tf = Terraform(working_dir=tmp_dir)
        self.tfstate_path = os.path.join(tmp_dir, "terraform.tfstate")
        self._run("init")

    def _run(self, cmd: str, *args, **kwargs):
        kwargs["capture_output"] = False
        ret, _stdout, _stderr = self.tf.cmd(cmd, *args, **kwargs)
        if ret != 0:
            raise TerraformError("Cmd 'terraform {cmd}' failed")

    def _load_state(self) -> dict:
        import json

        if not os.path.exists(self.tfstate_path):
            return {}
        with open(self.tfstate_path, encoding="utf-8") as fobj:
            return json.load(fobj)

    def init(self, **config):
        from python_terraform import IsFlagged

        from dvc.tpi import render_json

        assert "name" in config and "cloud" in config
        tf_file = os.path.join(self.tmp_dir, "main.tf.json")
        with open(tf_file, "w", encoding="utf-8") as fobj:
            fobj.write(render_json(**config, indent=2))
        self._run("init")
        self._run("apply", auto_approve=IsFlagged)

    def destroy(self, **config):
        from python_terraform import IsFlagged

        self._run("destroy", auto_approve=IsFlagged)

    def instances(self, **config) -> Iterable[dict]:
        try:
            name = config["name"]
        except KeyError:
            raise DvcException("Invalid machine")
        state = self._load_state()
        for resource in state.get("resources", []):
            if (
                resource.get("type") == "iterative_machine"
                and resource.get("name") == name
            ):
                yield from (
                    instance.get("attributes", {})
                    for instance in resource.get("instances", [])
                )
