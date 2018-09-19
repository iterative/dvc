import os

from dvc.command.base import CmdBase
from dvc.command.status import CmdDataStatus
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
                stages = self.project.reproduce(
                                       target,
                                       recursive=recursive,
                                       force=self.args.force,
                                       dry=self.args.dry,
                                       interactive=self.args.interactive,
                                       pipeline=self.args.pipeline)

                if len(stages) == 0:
                    self.project.logger.info(CmdDataStatus.UP_TO_DATE_MSG)

                if self.args.metrics:
                    self.project.metrics_show()
            except DvcException as ex:
                msg = 'Failed to reproduce \'{}\''.format(target)
                self.project.logger.error(msg, ex)
                ret = 1
                break

        os.chdir(saved_dir)
        return ret
