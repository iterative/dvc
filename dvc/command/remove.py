from dvc.exceptions import DvcException
from dvc.command.base import CmdBase
from dvc.logger import Logger


class CmdRemove(CmdBase):
    def run(self):
        outs_only = not self.args.purge
        for target in self.args.targets:
            try:
                self.project.remove(target, outs_only=outs_only)
            except DvcException as ex:
                Logger.error('Failed to remove {}'.format(target), ex)
                return 1
        return 0
