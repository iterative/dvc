from dvc.exceptions import DvcException
from dvc.command.common.base import CmdBase


class CmdRemove(CmdBase):
    def run(self):
        outs_only = not self.args.purge
        for target in self.args.targets:
            try:
                self.project.remove(target, outs_only=outs_only)
            except DvcException as ex:
                self.project.logger.error('Failed to remove {}'.format(target), ex)
                return 1
        return 0
