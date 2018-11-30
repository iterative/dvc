from dvc.exceptions import DvcException
from dvc.command.base import CmdBase


class CmdLockBase(CmdBase):
    def run(self, unlock=False):
        for target in self.args.targets:
            try:
                self.project.lock_stage(target, unlock=unlock)
            except DvcException as ex:
                msg = 'Failed to {}lock \'{}\''
                msg = msg.format('un' if unlock else '', target)
                self.project.logger.error(msg, ex)
                return 1
        return 0


class CmdLock(CmdLockBase):
    def run(self):
        return super(CmdLock, self).run(False)


class CmdUnlock(CmdLockBase):
    def run(self):
        return super(CmdUnlock, self).run(True)
