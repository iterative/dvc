import os

from dvc.command.common.base import CmdBase
from dvc.project import ReproductionError, StageNotFoundError
from dvc.stage import Stage


class CmdRepro(CmdBase):
    def run(self):
        recursive = not self.args.single_item
        saved_dir = os.path.realpath(os.curdir)
        if self.args.cwd:
            os.chdir(self.args.cwd)

        ret = 0
        for target in self.args.targets:
            try:
                self.project.reproduce(target,
                                       recursive=recursive,
                                       force=self.args.force)
            except ReproductionError as ex:
                msg = 'Failed to reproduce \'{}\''.format(target)
                self.project.logger.error(msg, ex)
                ret = 1
                break
            except StageNotFoundError as ex:
                if os.path.exists(target):
                    msg = '\'{}\' is not a dvc file.'.format(target)
                    dvcfile = target + Stage.STAGE_FILE_SUFFIX
                    if os.path.exists(target) and os.path.isfile(dvcfile):
                        msg += ' Maybe you meant \'{}\'?'.format(dvcfile)
                else:
                    msg = '\'{}\' does not exist.'.format(target)
                self.project.logger.error(msg)
                ret = 1
                break
            except Exception as ex:
                msg = 'Failed to reproduce \'{}\' - unexpected error'.format(target)
                self.project.logger.error(msg, ex)
                ret = 1
                break

        os.chdir(saved_dir)
        return ret
