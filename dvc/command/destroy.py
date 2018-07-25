from dvc.command.base import CmdBase
from dvc.prompt import prompt
from dvc.exceptions import DvcException


class CmdDestroy(CmdBase):
    def run_cmd(self):
        try:
            msg = u'This will destroy all information about your pipelines ' \
                  u'as well as cache in .dvc/cache.\n' \
                  u'Are you sure you want to continue?'

            if not self.args.force and not prompt(msg, False):
                msg = u'Cannot destroy without a confirmation from the ' \
                      u'user. Use \'-f\' to force.'
                raise DvcException(msg)

            self.project.destroy()
        except Exception as exc:
            self.project.logger.error('Failed to destroy DVC', exc)
            return 1
        return 0
