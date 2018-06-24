from dvc.exceptions import DvcException
from dvc.command.common.base import CmdBase


class CmdMove(CmdBase):
    def run(self):
        try:
            self.project.move(self.args.src, self.args.dst)
        except DvcException as ex:
            self.project.logger.error('Failed to move \'{}\' -> \'{}\''.format(self.args.src,
                                                                               self.args.dst), ex)
            return 1
        return 0
