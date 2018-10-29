from dvc.exceptions import DvcException
from dvc.command.base import CmdBase
from dvc.logger import Logger


class CmdRemove(CmdBase):
    def _is_outs_only(self, target):
        if not self.args.purge:
            return True

        if self.args.force:
            return False

        msg = (
            u'Are you sure you want to remove {} with its outputs?'
            .format(target)
        )

        confirmed = self.project.prompt.prompt(msg)

        if confirmed:
            return False

        raise DvcException(
            u'Cannot purge without a confirmation from the user.'
            u" Use '-f' to force."
        )

    def run(self):
        for target in self.args.targets:
            try:
                outs_only = self._is_outs_only(target)
                self.project.remove(target, outs_only=outs_only)
            except DvcException as ex:
                Logger.error('Failed to remove {}'.format(target), ex)
                return 1
        return 0
