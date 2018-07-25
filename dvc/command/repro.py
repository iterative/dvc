import os

from dvc.command.base import CmdBase


class CmdRepro(CmdBase):
    def run(self):
        from dvc.project import ReproductionError

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
                if self.args.metrics:
                    self.project.metrics_show()
            except ReproductionError as ex:
                msg = 'Failed to reproduce \'{}\''.format(target)
                self.project.logger.error(msg, ex)
                ret = 1
                break
            except Exception as ex:
                msg = 'Failed to reproduce \'{}\' - unexpected error'
                self.project.logger.error(msg.format(target), ex)
                ret = 1
                break

        os.chdir(saved_dir)
        return ret
