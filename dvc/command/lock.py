import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase


class CmdLockBase(CmdBase):
    def run(self, unlock=False):
        for target in self.args.targets:
            try:
                self.project.lock_stage(target, unlock=unlock)
            except DvcException:
                logger.error(
                    "failed to {}lock '{}'"
                    .format('un' if unlock else '', target)
                )

                return 1
        return 0


class CmdLock(CmdLockBase):
    def run(self):
        return super(CmdLock, self).run(False)


class CmdUnlock(CmdLockBase):
    def run(self):
        return super(CmdUnlock, self).run(True)
