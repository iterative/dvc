import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase


class CmdMove(CmdBase):
    def run(self):
        try:
            self.project.move(self.args.src, self.args.dst)
        except DvcException:
            msg = "failed to move '{}' -> '{}'".format(self.args.src,
                                                       self.args.dst)
            logger.error(msg)
            return 1
        return 0
