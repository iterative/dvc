from dvc.command.common.base import CmdBase
from dvc.prompt import prompt


class CmdDestroy(CmdBase):
    def run_cmd(self):
        try:
            msg = u'This will destroy all information about your pipelines as ' \
                  u'well as cache in .dvc/cache.\n' \
                  u'Are you sure you want to continue?'

            if not self.args.force and not prompt(msg, False):
                err = u'Cannot destroy without a confirmation from the user. ' \
                      u'Use \'-f\' to force.'
                self.project.logger.error(err)
                return 1

            self.project.destroy()
        except Exception as exc:
            self.project.logger.error('Failed to destroy DVC', exc)
            return 1
        return 0
