from dvc.project import Project
from dvc.logger import Logger
from dvc.command.common.base import CmdBase


class CmdInstall(CmdBase):
    def run_cmd(self):
        try:
            self.project.install()
        except Exception as e:
            Logger.error('Failed to install dvc hooks', e)
            return 1
        return 0
