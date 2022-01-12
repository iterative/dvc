import logging
import os
from typing import List

from dvc.cli.command import CmdBaseNoRepo

logger = logging.getLogger(__name__)


class CmdExternal(CmdBaseNoRepo):
    def exec(self, executable: str, args: List[str]) -> int:
        import subprocess

        return subprocess.call([executable, *args])

    def run(self) -> int:
        executable: str = self.args.executable
        args: List[str] = self.args.args
        cmd, _ = os.path.splitext(os.path.basename(executable))

        logger.debug("exec: %s", " ".join([cmd, *args]))
        return self.exec(executable, args)
