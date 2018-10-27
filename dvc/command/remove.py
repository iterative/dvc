from dvc.exceptions import DvcException
from dvc.command.base import CmdBase
from dvc.logger import Logger


class CmdRemove(CmdBase):
    def _is_purge_confirmed(self):
        msg = (
            'Are you sure you want to remove the following files'
            ' with their outputs? {}'
            .format(self.args.targets)
        )

        return self.args.purge and self.project.prompt.prompt(msg)

    def run(self):
        outs_only = not self._is_purge_confirmed()
        for target in self.args.targets:
            try:
                self.project.remove(target, outs_only=outs_only)
            except DvcException as ex:
                Logger.error('Failed to remove {}'.format(target), ex)
                return 1
        return 0
