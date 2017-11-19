import fasteners

from dvc.command.common.common_error import CmdCommonError


class DvcLockerError(CmdCommonError):
    def __init__(self, msg):
        super(DvcLockerError, self).__init__('DVC locker error: {}'.format(msg))


class DvcLock(object):
    TIMEOUT = 5

    def __init__(self, is_locker, git):
        self.is_locker = is_locker
        self.git = git
        self.lock = None

    def __enter__(self):
        if self.is_locker:
            self.lock = fasteners.InterProcessLock(self.git.lock_file)
            if not self.lock.acquire(timeout=self.TIMEOUT):
                raise DvcLockerError('Cannot perform the cmd since DVC is busy and locked. Please retry the cmd later.')
        return self.lock

    def __exit__(self, type, value, traceback):
        if self.is_locker:
            self.lock.release()
