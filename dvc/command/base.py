import argparse
import fasteners

from multiprocessing import cpu_count

from dvc.config import ConfigError
from dvc.exceptions import DvcException
from dvc.logger import Logger


class CmdBaseError(DvcException):
    def __init__(self, msg):
        super(CmdBaseError, self).__init__('{}'.format(msg))


class DvcLockerError(CmdBaseError):
    def __init__(self, msg):
        super(DvcLockerError, self).__init__('DVC locker error: {}'.format(msg))


class DvcLock(object):
    def __init__(self, is_locker, git):
        self.is_locker = is_locker
        self.git = git
        self.lock = None

    def __enter__(self):
        if self.is_locker:
            self.lock = fasteners.InterProcessLock(self.git.lock_file)
            if not self.lock.acquire(timeout=5):
                raise DvcLockerError('Cannot perform the cmd since DVC is busy and locked. Please retry the cmd later.')
        return self.lock

    def __exit__(self, type, value, traceback):
        if self.is_locker:
            self.lock.release()


class CmdBase(object):
    def __init__(self, settings):
        self._settings = settings

        if settings._parsed_args.quiet and not settings._parsed_args.verbose:
            Logger.be_quiet()
        elif not settings._parsed_args.quiet and settings._parsed_args.verbose:
            Logger.be_verbose()

    @property
    def settings(self):
        return self._settings

    #NOTE: this name is really confusing. It should really be called "command" or smth,
    # because it is only used for "command" argument from CmdRun.
    @property
    def args(self):
        return self._settings.args

    @property
    def cloud(self):
        return self._settings.cloud

    @property
    def parsed_args(self):
        return self._settings._parsed_args

    @property
    def config(self):
        return self._settings.config

    @property
    def dvc_home(self):
        return self._settings.dvc_home

    @property
    def git(self):
        return self._settings.git

    @property
    def no_git_actions(self):
        return self.parsed_args.no_git_actions

    def set_git_action(self, value):
        self.parsed_args.no_git_actions = not value

    @property
    def is_locker(self):
        if 'no_lock' in self.parsed_args.__dict__:
            return not self.parsed_args.no_lock
        return True

    def set_locker(self, value):
        self.parsed_args.no_lock = value

    def commit_if_needed(self, message, error=False):
        if error or self.no_git_actions:
            self.not_committed_changes_warning()
            return 1
        else:
            self.git.commit_all_changes_and_log_status(message)
            return 0

    @staticmethod
    def not_committed_changes_warning():
        Logger.warn('changes were not committed to git')

    def run(self):
        pass
