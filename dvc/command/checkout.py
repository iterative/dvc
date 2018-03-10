import os
import stat

from dvc.system import System
from dvc.command.common.base import CmdBase
from dvc.logger import Logger


class CmdCheckout(CmdBase):
    def run(self):
        self.project.checkout()
        return 0
