import os
import schema
import posixpath
import ntpath

from dvc.dependency.base import *
from dvc.logger import Logger
from dvc.utils import remove
from dvc.cloud.local import DataCloudLOCAL
from dvc.config import Config
from dvc.remote.local import RemoteLOCAL


class DependencyLOCAL(DependencyBase):
    REGEX = r'^(?P<path>(/+|.:\\+)?.*)$'

    DoesNotExistError = DependencyDoesNotExistError
    IsNotFileOrDirError = DependencyIsNotFileOrDirError

    def __init__(self, stage, path, info=None):
        self.stage = stage
        self.project = stage.project
        if not os.path.isabs(path):
            path = self.unixpath(path)
            path = os.path.join(stage.cwd, path)
        self.path = os.path.normpath(path)
        self.info = info
        self.remote = RemoteLOCAL(stage.project,
                        {Config.SECTION_REMOTE_URL: self.project.dvc_dir})
        self.path_info = {'scheme': 'local',
                          'path': self.path}

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

    @staticmethod
    def unixpath(path):
        assert not ntpath.isabs(path)
        assert not posixpath.isabs(path)
        return path.replace('\\', '/')

    def dumpd(self):
        if self.path.startswith(self.stage.project.root_dir):
            path = self.unixpath(os.path.relpath(self.path, self.stage.cwd))
        else:
            path = self.path

        info = self.info
        info[self.PARAM_PATH] = path
        return info
