from dvc.exceptions import DvcException
from dvc.command.common.base import CmdBase

class CmdRepro(CmdBase):
    def run(self):
        recursive = not self.args.single_item
        for target in self.args.targets:
            try:
                self.project.reproduce(target,
                                       recursive=recursive,
                                       force=self.args.force)
            except DvcException as ex:
                msg = 'Failed to reproduce {}: {}'.format(target, str(ex))
                self.project.logger.error(msg)
                return 1
        return 0
