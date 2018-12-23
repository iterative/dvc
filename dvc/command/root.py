import os

import dvc.logger as logger
from dvc.command.base import CmdBase


class CmdRoot(CmdBase):
    def run_cmd(self):
        return self.run()

    def run(self):
        logger.info(os.path.relpath(self.project.root_dir))
        return 0
