from dvc.exceptions import DvcException
from dvc.command.base import CmdBase
from dvc.logger import Logger


class CmdRemove(CmdBase):
    def _confirm_removal(self, target):
        if self.args.force:
            return

        confirmed = self.project.prompt.prompt(
            u'Are you sure you want to remove {} with its outputs?'
            .format(target)
        )

        if confirmed:
            return

        raise DvcException(
            u'Cannot remove without a confirmation from the user.'
            u" Use '-f' to force."
        )

    def run(self):
        for target in self.args.targets:
            try:
                outs_only = self.args.outs

                if not outs_only:
                    self._confirm_removal(target)

                self.project.remove(target, outs_only=outs_only)

            except DvcException as ex:
                Logger.error('Failed to remove {}'.format(target), ex)
                return 1
        return 0
