import os

from dvc.command.base import CmdBase


class CmdRoot(CmdBase):
    def run(self):
        self.project.logger.info(os.path.relpath(self.project.root_dir))
        return 0
