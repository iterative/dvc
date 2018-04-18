import os

from dvc.command.common.base import CmdBase
from dvc.project import ReproductionError, StageNotFoundError
from dvc.stage import Stage


class CmdRepro(CmdBase):
    def run(self):
        recursive = not self.args.single_item
        for target in self.args.targets:
            try:
                self.project.reproduce(target,
                                       recursive=recursive,
                                       force=self.args.force)
            except ReproductionError as ex:
                msg = 'Failed to reproduce \'{}\''.format(target)
                self.project.logger.error(msg, ex)
                return 1
            except StageNotFoundError as ex:
                if os.path.exists(target):
                    msg = '\'{}\' is not a dvc file.'.format(target)
                    dvcfile = target + Stage.STAGE_FILE_SUFFIX
                    if os.path.exists(target) and os.path.isfile(dvcfile):
                        msg += ' Maybe you meant \'{}\'?'.format(dvcfile)
                else:
                    msg = '\'{}\' does not exist.'.format(target)
                self.project.logger.error(msg)
                return 1
            except Exception as ex:
                msg = 'Failed to reproduce \'{}\' - unexpected error'.format(target)
                self.project.logger.error(msg, ex)
                return 1
        return 0
