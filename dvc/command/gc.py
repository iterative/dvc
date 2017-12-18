from dvc.command.common.base import CmdBase


class CmdGC(CmdBase):
    def run(self):
        self.project.gc()
        return 0
