import os
import schema
import posixpath
import ntpath

from dvc.dependency import Dependency
from dvc.exceptions import DvcException
from dvc.logger import Logger
from dvc.utils import remove


class OutputError(DvcException):
    def __init__(self, path, msg):
        super(OutputError, self).__init__('Output \'{}\' error: {}'.format(path, msg))


class OutputOutsideOfRepoError(OutputError):
    def __init__(self, path):
        super(OutputOutsideOfRepoError, self).__init__(path, 'outside of repository')


class OutputDoesNotExistError(OutputError):
    def __init__(self, path):
        super(OutputDoesNotExistError, self).__init__(path, 'does not exist')


class OutputIsNotFileOrDirError(OutputError):
    def __init__(self, path):
        super(OutputIsNotFileOrDirError, self).__init__(path, 'not a file or directory')


class OutputAlreadyTrackedError(OutputError):
    def __init__(self, path):
        super(OutputAlreadyTrackedError, self).__init__(path, 'already tracked by scm(e.g. git)')


class Output(Dependency):
    PARAM_CACHE = 'cache'

    OutsideOfRepoError = OutputOutsideOfRepoError                                        
    DoesNotExistError = OutputDoesNotExistError                                          
    IsNotFileOrDirError = OutputIsNotFileOrDirError                                      

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
            raise OutputAlreadyTrackedError(self.rel_path)

        if not self.changed():
             return

        self.project.cache.save(self.path)

    def remove(self):
        remove(self.path)
