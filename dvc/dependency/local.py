import os
import schema
import posixpath
import ntpath

from dvc.dependency.base import *
from dvc.logger import Logger
from dvc.utils import remove
from dvc.cloud.local import DataCloudLOCAL


class DependencyLOCAL(DependencyBase):
    REGEX = r'^(?P<path>(/+|.:\\+)?.*)$'

    PARAM_PATH = 'path'
    PARAM_MD5 = 'md5'
    MD5_DIR_SUFFIX = '.dir'

    DoesNotExistError = DependencyDoesNotExistError
    IsNotFileOrDirError = DependencyIsNotFileOrDirError

    def __init__(self, stage, path, md5=None):
        self.stage = stage
        self.project = stage.project
        self.path = os.path.abspath(os.path.normpath(path))
        self.md5 = md5

    @property
    def rel_path(self):
        return os.path.relpath(self.path)

    def _changed_md5(self):
        if not os.path.exists(self.path):
            return True

        return self.project.state.changed(self.path, self.md5)

    @staticmethod
    def _changed_msg(changed):
        if changed:
            return 'changed'
        return "didn't change"

    def changed(self):
        ret = self._changed_md5()

        msg = u'Dependency \'{}\' {}'.format(self.rel_path, self._changed_msg(ret))
        self.project.logger.debug(msg)

        return ret

    def save(self):
        if not os.path.exists(self.path):
            raise self.DoesNotExistError(self.rel_path)

        if not os.path.isfile(self.path) and not os.path.isdir(self.path):
            raise self.NotFileOrDirError(self.rel_path)

        if (os.path.isfile(self.path) and os.path.getsize(self.path) == 0) or \
           (os.path.isdir(self.path) and len(os.listdir(self.path)) == 0):
            self.project.logger.warn("File/directory '{}' is empty.".format(self.rel_path))

        self.md5 = self.project.state.update(self.path)

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

        return {self.PARAM_PATH: path,
                self.PARAM_MD5:self.md5}

    @classmethod
    def loadd(cls, stage, d):
        path = d[cls.PARAM_PATH]
        if not os.path.isabs(path):
            path = cls.unixpath(path)
            path = os.path.join(stage.cwd, path)
        path = os.path.normpath(path)
        md5 = d.get(cls.PARAM_MD5, None)
        return cls(stage, path, md5=md5)

    @classmethod
    def loads(cls, stage, s):
        return cls(stage, os.path.join(stage.cwd, s), md5=None)
