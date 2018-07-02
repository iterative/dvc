from dvc.exceptions import DvcException
from dvc.command.common.base import CmdBase


class CmdAdd(CmdBase):
    def run(self):
        for target in self.args.targets:
            try:
                self.project.add(target)
            except DvcException as ex:
                self.project.logger.error('Failed to add \'{}\''.format(target), ex)
                return 1
        return 0
