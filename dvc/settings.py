import os

from dvc.git_wrapper import GitWrapperI, GitWrapper
from dvc.config import ConfigI, Config
from dvc.exceptions import DvcException
from dvc.path.factory import PathFactory
from dvc.cli import parse_args
from dvc.data_cloud import DataCloud

class SettingsError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, msg)


class Settings(object):
    def __init__(self, argv=None, git=None, config=None):
        self._args = None
        args = None

        if argv is not None and len(argv) != 0:
            args = parse_args(argv)
            self._args = argv[2:]

        if git is None:
            git = GitWrapper()

        if config is None:
            if args is not None and args.cmd != 'init':
                config = Config()
            else:
                config = ConfigI()

        self._git = git
        self._config = config
        self._path_factory = PathFactory(git, config)
        self._parsed_args = args
        self._cloud = DataCloud(self)

    @property
    def args(self):
        return self._args

    def set_args(self, args):
        self._args = args

    @property
    def parsed_args(self):
        return self._parsed_args

    def parse_args(self, args):
        self._parsed_args = parse_args(args)

    @property
    def git(self):
        return self._git

    @property
    def config(self):
        return self._config

    @property
    def dvc_home(self):
        return self._dvc_home

    @property
    def path_factory(self):
        return self._path_factory

    @property
    def cloud(self):
        return self._cloud
