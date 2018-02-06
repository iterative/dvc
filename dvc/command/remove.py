from dvc.exceptions import DvcException
from dvc.command.common.base import CmdBase


class CmdRemove(CmdBase):
    def run(self):
        for target in self.args.targets:
            try:
                self.project.remove(target)
            except DvcException as ex:
                self.project.logger.error('Failed to remove {}', ex)
                return 1
        return 0
