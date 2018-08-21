from dvc.exceptions import DvcException
from dvc.command.base import CmdBase
from dvc.logger import Logger


class CmdAdd(CmdBase):
    def run(self):
        for target in self.args.targets:
            try:
                self.project.add(target, recursive=self.args.recursive)
            except DvcException as ex:
                Logger.error('Failed to add \'{}\''.format(target), ex)
                return 1
        return 0
