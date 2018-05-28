import os
import schema
import posixpath
import ntpath

from dvc.output.base import *
from dvc.dependency.local import DependencyLOCAL
from dvc.exceptions import DvcException
from dvc.logger import Logger
from dvc.utils import remove


class OutputLOCAL(DependencyLOCAL):
    PARAM_CACHE = 'cache'

    DoesNotExistError = OutputDoesNotExistError                                          
    IsNotFileOrDirError = OutputIsNotFileOrDirError                                      

    def __init__(self, stage, path, md5=None, use_cache=True):
        super(OutputLOCAL, self).__init__(stage, path, md5=md5)
        self.use_cache = use_cache

    @property
    def cache(self):
        if not self.use_cache:
            return None

        return self.project.cache.get(self.md5)

    def dumpd(self):
        ret = super(OutputLOCAL, self).dumpd()
        ret[self.PARAM_CACHE] = self.use_cache
        return ret

    @classmethod
    def loadd(cls, stage, d):
        ret = super(OutputLOCAL, cls).loadd(stage, d)
        ret.use_cache = d.get(cls.PARAM_CACHE, True)
        return ret

    @classmethod
    def loads(cls, stage, s, use_cache=True):
        ret = super(OutputLOCAL, cls).loads(stage, s)
        ret.use_cache = use_cache
        return ret

    def changed(self):
        if super(OutputLOCAL, self).changed():
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
        super(OutputLOCAL, self).save()

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
