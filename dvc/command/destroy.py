import dvc.prompt as prompt
import dvc.logger as logger
from dvc.command.base import CmdBase
from dvc.exceptions import DvcException


class CmdDestroy(CmdBase):
    def run_cmd(self):
        try:
            statement = (
                'This will destroy all information about your pipelines,'
                ' all data files, as well as cache in .dvc/cache.'
                '\n'
                'Are you sure you want to continue?'
            )

            if not self.args.force and not prompt.confirm(statement):
                raise DvcException(
                    "cannot destroy without a confirmation from the user."
                    " Use '-f' to force."
                )

            self.project.destroy()
        except Exception:
            logger.error('failed to destroy DVC')
            return 1
        return 0
