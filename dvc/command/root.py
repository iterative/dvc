import os

from dvc.command.base import CmdBase


class CmdRoot(CmdBase):
    def run_cmd(self):
        return self.run()

    def run(self):
        self.project.logger.info(os.path.relpath(self.project.root_dir))
        return 0
