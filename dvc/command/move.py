from dvc.exceptions import DvcException
from dvc.command.base import CmdBase


class CmdMove(CmdBase):
    def run(self):
        try:
            self.project.move(self.args.src, self.args.dst)
        except DvcException as ex:
            msg = 'Failed to move \'{}\' -> \'{}\''.format(self.args.src,
                                                           self.args.dst)
            self.project.logger.error(msg, ex)
            return 1
        return 0
