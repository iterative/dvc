import dvc.logger as logger
from dvc.command.base import CmdBase


class CmdInstall(CmdBase):
    def run_cmd(self):
        try:
            self.project.install()
        except Exception:
            logger.error('failed to install dvc hooks')
            return 1
        return 0
