import os
import schema
import posixpath
import ntpath

from dvc.exceptions import DvcException
from dvc.logger import Logger
from dvc.utils import remove


class DependencyError(DvcException):
    def __init__(self, path, msg):
        super(DependencyError, self).__init__('Dependency \'{}\' error: {}'.format(path, msg))


class DependencyOutsideOfRepoError(DependencyError):
    def __init__(self, path):
        super(DependencyOutsideOfRepoError, self).__init__(path, 'outside of repository')


class DependencyDoesNotExistError(DependencyError):
    def __init__(self, path):
        super(DependencyDoesNotExistError, self).__init__(path, 'does not exist')


class DependencyIsNotFileOrDirError(DependencyError):
    def __init__(self, path):
        super(DependencyIsNotFileOrDirError, self).__init__(path, 'not a file or directory')


class Dependency(object):
    PARAM_PATH = 'path'
    PARAM_MD5 = 'md5'
    MD5_DIR_SUFFIX = '.dir'

    OutsideOfRepoError = DependencyOutsideOfRepoError
    DoesNotExistError = DependencyDoesNotExistError
    IsNotFileOrDirError = DependencyIsNotFileOrDirError

    SCHEMA = {
        PARAM_PATH: str,
        schema.Optional(PARAM_MD5): schema.Or(str, None),
    }

    def __init__(self, stage, path, md5=None):
        self.stage = stage
        self.project = stage.project
        self.path = os.path.abspath(os.path.normpath(path))

        if not self.path.startswith(self.project.root_dir):
            raise self.OutsideOfRepoError(self.rel_path)

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

    def status(self):
        if self.changed():
            #FIXME better msgs
            return {self.rel_path: 'changed'}
        return {}

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
        return {
            self.PARAM_PATH: self.unixpath(os.path.relpath(self.path, self.stage.cwd)),
            self.PARAM_MD5: self.md5,
        }

    @classmethod
    def loadd(cls, stage, d):
        relpath = os.path.normpath(Dependency.unixpath(d[Dependency.PARAM_PATH]))
        path = os.path.join(stage.cwd, relpath)
        md5 = d.get(Dependency.PARAM_MD5, None)
        return cls(stage, path, md5=md5)

    @classmethod
    def loadd_from(cls, stage, d_list):
        return [cls.loadd(stage, x) for x in d_list]

    @classmethod
    def loads(cls, stage, s):
        return cls(stage, os.path.join(stage.cwd, s), md5=None)

    @classmethod
    def loads_from(cls, stage, s_list):
        return [cls.loads(stage, x) for x in s_list]
