import os
import schema
import posixpath
import ntpath

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.dependency.base import *
from dvc.logger import Logger
from dvc.utils import remove
from dvc.config import Config
from dvc.remote.local import RemoteLOCAL


class DependencyLOCAL(DependencyBase):
    REGEX = r'^(?P<path>(/+|.:\\+)?.*)$'

    DoesNotExistError = DependencyDoesNotExistError
    IsNotFileOrDirError = DependencyIsNotFileOrDirError

    def __init__(self, stage, path, info=None, remote=None):
        self.stage = stage
        self.project = stage.project
        self.info = info
        self.remote = remote if remote != None else RemoteLOCAL(stage.project, {})

        if remote:
            path = os.path.join(remote.prefix, urlparse(path).path.lstrip('/'))

        if not os.path.isabs(path):
            path = self.ospath(path)
            path = os.path.join(stage.cwd, path)
        self.path = os.path.abspath(os.path.normpath(path))

        self.path_info = {'scheme': 'local',
                          'path': self.path}

    @property
    def sep(self):
        return os.sep

    @property
    def rel_path(self):
        return os.path.relpath(self.path)

    def changed(self):
        if not os.path.exists(self.path):
            return True

        info = self.remote.save_info(self.path_info)

        return self.info != info

    def save(self):
        if not os.path.exists(self.path):
            raise self.DoesNotExistError(self.rel_path)

        if not os.path.isfile(self.path) and not os.path.isdir(self.path):
            raise self.NotFileOrDirError(self.rel_path)

        if (os.path.isfile(self.path) and os.path.getsize(self.path) == 0) or \
           (os.path.isdir(self.path) and len(os.listdir(self.path)) == 0):
            self.project.logger.warn("File/directory '{}' is empty.".format(self.rel_path))

        self.info = self.remote.save_info(self.path_info)

    def ospath(self, path):
        if os.name == 'nt':
            return self.ntpath(path)
        return self.unixpath(path)

    @staticmethod
    def unixpath(path):
        assert not ntpath.isabs(path)
        assert not posixpath.isabs(path)
        return path.replace('\\', '/')

    @staticmethod
    def ntpath(path):
        assert not ntpath.isabs(path)
        assert not posixpath.isabs(path)
        return path.replace('/', '\\')

    def dumpd(self):
        if self.path.startswith(self.stage.project.root_dir):
            path = self.unixpath(os.path.relpath(self.path, self.stage.cwd))
        else:
            path = self.path

        info = self.info
        info[self.PARAM_PATH] = path
        return info
