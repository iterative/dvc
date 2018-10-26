from dvc.exceptions import DvcException
from dvc.command.base import CmdBase


class CmdUnprotect(CmdBase):
    def run(self):
        for target in self.args.targets:
            try:
                self.project.unprotect(target)
            except DvcException as ex:
                msg = "Failed to unprotect '{}'".format(target)
                self.project.logger.error(msg, ex)
                return 1
        return 0
