from dvc.exceptions import DvcException
from dvc.command.common.base import CmdBase


class CmdImport(CmdBase):
    def run(self):
        try:
            self.project.imp(self.args.url, self.args.out)
        except DvcException as ex:
            self.project.logger.error('Failed to import {}', ex)
            return 1
        return 0
