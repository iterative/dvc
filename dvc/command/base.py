import argparse
import os
import sys

from dvc.git_wrapper import GitWrapper
from dvc.config import Config, ConfigError
from dvc.logger import Logger
from dvc.path.factory import PathFactory
from dvc.utils import cached_property


class CmdBase(object):
    CONFIG = 'dvc.conf'

    def __init__(self, args=None, parse_config=True, git_obj=None, config_obj=None):
        if git_obj:
            self._git = git_obj
        else:
            self._git = GitWrapper()

        if config_obj:
            self._config = config_obj
        else:
            self._config = None
            if parse_config:
                self._config = Config(os.path.realpath(os.path.join(self.git.git_dir, self.CONFIG)))

        if args is None:
            self._args = sys.argv[1:]
        else:
            self._args = args

        self._parsed_args = None
        self._dvc_home = None

        parser = argparse.ArgumentParser()
        self.define_args(parser)
        self._parsed_args, self._command_args = parser.parse_known_args(args=self._args)

        self._dvc_home = os.environ.get('DVC_HOME')

        if not self.lnx_home:
            raise ConfigError('DVC_HOME environment variable is not defined')
        if not os.path.exists(self._dvc_home):
            raise ConfigError("DVC_HOME directory doesn't exists")
        pass

    @property
    def args(self):
        return self._args

    @property
    def parsed_args(self):
        return self._parsed_args

    @property
    def command_args(self):
        return self._command_args

    @cached_property
    def path_factory(self):
        return PathFactory(self.git, self.config)

    @property
    def config(self):
        return self._config

    def cache_file_aws_key(self, file):
        return '{}/{}'.format(self._config.aws_storage_prefix, file).strip('/')

    @property
    def lnx_home(self):
        return self._dvc_home

    @property
    def git(self):
        return self._git

    def define_args(self, parser):
        pass

    def set_skip_git_actions(self, parser):
        parser.add_argument('--skip-git-actions', '-s', action='store_true',
                            help='skip all git actions including reproducibility check and commits')

    @property
    def skip_git_actions(self):
        return self.parsed_args.skip_git_actions

    def commit_if_needed(self, message, error=False):
        if error or self.skip_git_actions:
            self.not_committed_changes_warning()
            return 1
        else:
            self.git.commit_all_changes_and_log_status(message)
            return 0

    @staticmethod
    def not_committed_changes_warning():
        Logger.warn('Warning: changes were not committed to git')

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

    def get_cache_file_s3_name(self, cache_file):
        cache_prefix_file_name = os.path.relpath(os.path.realpath(cache_file), os.path.realpath(self.git.git_dir))
        file_name = os.path.relpath(cache_prefix_file_name, self.config.cache_dir)
        dvc_file_path_trim = file_name.replace(os.sep, '/').strip('/')
        return self.config.aws_storage_prefix + '/' + dvc_file_path_trim

    @staticmethod
    def warning_dvc_is_busy():
        Logger.warn('Cannot perform the cmd since DVC is busy and locked. Please retry the cmd later.')
