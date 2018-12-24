import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase


class CmdUnprotect(CmdBase):
    def run(self):
        for target in self.args.targets:
            try:
                self.project.unprotect(target)
            except DvcException:
                msg = "failed to unprotect '{}'".format(target)
                logger.error(msg)
                return 1
        return 0
