import os

from dvc.command.base import CmdBase
from dvc.exceptions import DvcException


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
                                       force=self.args.force,
                                       dry=self.args.dry,
                                       interactive=self.args.interactive)
                if self.args.metrics:
                    self.project.metrics_show()
            except DvcException as ex:
                msg = 'Failed to reproduce \'{}\''.format(target)
                self.project.logger.error(msg, ex)
                ret = 1
                break

        os.chdir(saved_dir)
        return ret
