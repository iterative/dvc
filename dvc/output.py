import os
import schema
import posixpath
import ntpath

from dvc.exceptions import DvcException
from dvc.logger import Logger
from dvc.utils import remove


class OutputError(DvcException):
    pass


class CmdOutputError(DvcException):
    def __init__(self, path, msg):
        super(CmdOutputError, self).__init__('Output file \'{}\' error: {}'.format(path, msg))


class CmdOutputNoCacheError(CmdOutputError):
    def __init__(self, path):
        super(CmdOutputNoCacheError, self).__init__(path, 'no cache')


class CmdOutputOutsideOfRepoError(CmdOutputError):
    def __init__(self, path):
        super(CmdOutputOutsideOfRepoError, self).__init__(path, 'outside of repository')


class CmdOutputDoesNotExistError(CmdOutputError):
    def __init__(self, path):
        super(CmdOutputDoesNotExistError, self).__init__(path, 'does not exist')


class CmdOutputIsNotFileOrDirError(CmdOutputError):
    def __init__(self, path):
        super(CmdOutputIsNotFileOrDirError, self).__init__(path, 'not a file or directory')


class CmdOutputAlreadyTrackedError(CmdOutputError):
    def __init__(self, path):
        super(CmdOutputAlreadyTrackedError, self).__init__(path, 'already tracked by scm(e.g. git)')


class Dependency(object):
    PARAM_PATH = 'path'
    PARAM_MD5 = 'md5'
    MD5_DIR_SUFFIX = '.dir'

    SCHEMA = {
        PARAM_PATH: str,
        schema.Optional(PARAM_MD5): schema.Or(str, None),
    }

    def __init__(self, project, path, md5=None):
        self.project = project
        self.path = os.path.abspath(os.path.normpath(path))

        if not self.path.startswith(self.project.root_dir):
            raise CmdOutputOutsideOfRepoError(self.rel_path)

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
            raise CmdOutputDoesNotExistError(self.rel_path)

        if not os.path.isfile(self.path) and not os.path.isdir(self.path):
            raise CmdOutputIsNotFileOrDirError(self.rel_path)

        if (os.path.isfile(self.path) and os.path.getsize(self.path) == 0) or \
           (os.path.isdir(self.path) and len(os.listdir(self.path)) == 0):
            self.project.logger.warn("File/directory '{}' is empty.".format(self.rel_path))

        self.md5 = self.project.state.update(self.path)

    @staticmethod
    def unixpath(path):
        assert not ntpath.isabs(path)
        assert not posixpath.isabs(path)
        return path.replace('\\', '/')

    def dumpd(self, cwd):
        return {
            Output.PARAM_PATH: self.unixpath(os.path.relpath(self.path, cwd)),
            Output.PARAM_MD5: self.md5,
        }

    @classmethod
    def loadd(cls, project, d, cwd=os.curdir):
        relpath = os.path.normpath(Output.unixpath(d[Output.PARAM_PATH]))
        path = os.path.join(cwd, relpath)
        md5 = d.get(Output.PARAM_MD5, None)
        return cls(project, path, md5=md5)

    @classmethod
    def loadd_from(cls, project, d_list, cwd=os.curdir):
        return [cls.loadd(project, x, cwd=cwd) for x in d_list]

    @classmethod
    def loads(cls, project, s, cwd=os.curdir):
        return cls(project, os.path.join(cwd, s), md5=None)

    @classmethod
    def loads_from(cls, project, s_list, cwd=os.curdir):
        return [cls.loads(project, x, cwd=cwd) for x in s_list]


class Output(Dependency):
    PARAM_CACHE = 'cache'

    SCHEMA = Dependency.SCHEMA
    SCHEMA[schema.Optional(PARAM_CACHE)] = bool

    def __init__(self, project, path, md5=None, use_cache=True):
        super(Output, self).__init__(project, path, md5=md5)
        self.use_cache = use_cache

    @property
    def cache(self):
        if not self.use_cache:
            return None

        return self.project.cache.get(self.md5)

    def dumpd(self, cwd):
        ret = super(Output, self).dumpd(cwd)
        ret[Output.PARAM_CACHE] = self.use_cache
        return ret

    @classmethod
    def loadd(cls, project, d, cwd=os.curdir):
        ret = super(Output, cls).loadd(project, d, cwd=cwd)
        ret.use_cache = d.get(Output.PARAM_CACHE, True)
        return ret

    @classmethod
    def loads(cls, project, s, use_cache=True, cwd=os.curdir):
        ret = super(Output, cls).loads(project, s, cwd=cwd)
        ret.use_cache = use_cache
        return ret

    @classmethod
    def loads_from(cls, project, s_list, use_cache=False, cwd=os.curdir):
        return [cls.loads(project, x, use_cache=use_cache, cwd=cwd) for x in s_list]

    def changed(self):
        if super(Output, self).changed():
            return True

        if self.use_cache and self.project.cache.changed(self.md5):
            return True

        return False

    def checkout(self):
        if not self.use_cache:
            return

        msg = u'Checking out \'{}\' with cache \'{}\''
        self.project.logger.debug(msg.format(self.rel_path, self.md5))

        if not self.changed():
            msg = u'Data file \'{}\' with cache \'{}\' didn\'t change, skipping checkout.'
            self.project.logger.debug(msg.format(self.rel_path, self.md5))
            return

        self.project.cache.checkout(self.path, self.md5)

    def save(self):
        super(Output, self).save()

        if not self.use_cache:
            return

        self.project.logger.debug(u'Saving \'{}\' to \'{}\''.format(self.rel_path, self.md5))

        if self.project.scm.is_tracked(self.path):
            raise CmdOutputAlreadyTrackedError(self.rel_path)

        if not self.changed():
             return

        self.project.cache.save(self.path)

    def remove(self):
        remove(self.path)
