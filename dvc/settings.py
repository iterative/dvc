import os

from dvc.exceptions import DvcException
from dvc.path.factory import PathFactory


class SettingsError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, msg)


class Settings(object):
    def __init__(self, args, git, config):
        self._args = args
        self._git = git
        self._config = config
        self._path_factory = PathFactory(git, config)

        # self._dvc_home = os.environ.get('DVC_HOME')
        # if not self._dvc_home:
        #     raise SettingsError('DVC_HOME environment variable is not defined')
        # if not os.path.exists(self._dvc_home):
        #     raise SettingsError("DVC_HOME directory doesn't exists")
        pass

    @property
    def args(self):
        return self._args

    def set_args(self, args):
        self._args = args

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