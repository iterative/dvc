import os
import sys

from dvc.config import Config, ConfigI
from dvc.exceptions import DvcException
from dvc.git_wrapper import GitWrapper
from dvc.logger import Logger
from dvc.settings import Settings


class Runtime(object):
    CONFIG = 'dvc.conf'

    @staticmethod
    def conf_file_path(git_dir):
        return os.path.realpath(os.path.join(git_dir, Runtime.CONFIG))

    @staticmethod
    def run(cmd_class, parse_config=True):
        try:
            runtime_git = GitWrapper()

            if parse_config:
                runtime_config = Config(Runtime.conf_file_path(runtime_git.git_dir))
            else:
                runtime_config = ConfigI()

            args = sys.argv[1:]

            instance = cmd_class(Settings(args, runtime_git, runtime_config))
            sys.exit(instance.run())
        except DvcException as e:
            Logger.error(e)
            sys.exit(1)
