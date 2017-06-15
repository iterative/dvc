import argparse
import fasteners

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

        parser = argparse.ArgumentParser()
        self.define_common_args(parser)
        self.define_args(parser)

        self._parsed_args, self._command_args = parser.parse_known_args(args=self.args)

        self.process_common_args()

    @property
    def settings(self):
        return self._settings

    @property
    def args(self):
        return self._settings.args

    @property
    def parsed_args(self):
        return self._parsed_args

    @property
    def command_args(self):
        return self._command_args

    @property
    def config(self):
        return self._settings.config

    @property
    def dvc_home(self):
        return self._settings.dvc_home

    @property
    def git(self):
        return self._settings.git

    def define_args(self, parser):
        pass

    def set_no_git_actions(self, parser):
        parser.add_argument('--no-git-actions', '-G', action='store_true', default=False,
                            help='Skip all git actions including reproducibility check and commits.')

    def set_lock_action(self, parser):
        parser.add_argument('--lock', '-l', action='store_true', default=False,
                            help='Lock data item - disable reproduction. ' +
                                 'It can be enabled by `dvc lock` command or by forcing reproduction.')

    def define_common_args(self, parser):
        parser.add_argument('--quiet', '-q', action='store_true', default=False, help='Be quiet.')
        parser.add_argument('--verbose', '-v', action='store_true', default=False, help='Be verbose.')

    def process_common_args(self):
        if self._parsed_args.quiet and not self._parsed_args.verbose:
            Logger.be_quiet()
        elif not self._parsed_args.quiet and self._parsed_args.verbose:
            Logger.be_verbose()

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

    def add_string_arg(self, parser, name, message, default = None,
                       conf_section=None, conf_name=None):
        if conf_section and conf_name:
            section = self.config[conf_section]
            if not section:
                raise ConfigError("")
            default_value = section.get(conf_section, default)
        else:
            default_value = default

        parser.add_argument(name,
                            metavar='',
                            default=default_value,
                            help=message)

    def run(self):
        pass

    @staticmethod
    def warning_dvc_is_busy():
        Logger.warn('Cannot perform the cmd since DVC is busy and locked. Please retry the cmd later.')
