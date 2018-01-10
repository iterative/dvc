from dvc.command.common.base import CmdBase
from dvc.project import ReproductionError


class CmdRepro(CmdBase):
    def run(self):
        recursive = not self.args.single_item
        for target in self.args.targets:
            try:
                self.project.reproduce(target,
                                       recursive=recursive,
                                       force=self.args.force)
            except ReproductionError as ex:
                self.project.logger.error(ex)
            except Exception as ex:
                msg = 'Failed to reproduce \'{}\' - unexpected error: {}'.format(target, ex)
                self.project.logger.error(msg)
                return 1
        return 0
