import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase


class CmdAdd(CmdBase):
    def run(self):
        for target in self.args.targets:
            try:
                self.project.add(target, recursive=self.args.recursive)
            except DvcException:
                logger.error('failed to add file')
                return 1
        return 0
