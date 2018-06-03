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

    def __init__(self, stage, path, info=None, cache=True):
        super(OutputLOCAL, self).__init__(stage, path, info)
        self.use_cache = cache

    @property
    def md5(self):
        #FIXME
        return self.info.get(self.project.cache.local.PARAM_MD5, None)

    @property
    def cache(self):
        return self.project.cache.local.get(self.md5)

    def dumpd(self):
        ret = super(OutputLOCAL, self).dumpd()
        ret[self.PARAM_CACHE] = self.use_cache
        return ret

    def changed(self):
        if super(OutputLOCAL, self).changed():
            return True

        if self.use_cache and self.info != self.project.cache.local.save_info(self.path_info):
            return True

        return False

    def checkout(self):
        if not self.use_cache:
            return

        if not self.changed():
            msg = u'Data file \'{}\' didn\'t change, skipping checkout.'
            self.project.logger.debug(msg.format(self.rel_path))
            return

        self.project.cache.local.checkout(self.path_info, self.info)

    def save(self):
        if not self.changed():
            msg = 'Output \'{}\' didn\'t change. Skipping saving.'
            self.project.logger.debug(msg.format(self.rel_path))
            return

        super(OutputLOCAL, self).save()

        if not self.use_cache:
            msg = 'Output \'{}\' doesn\'t use cache. Skipping saving.'
            self.project.logger.debug(msg.format(self.rel_path))
            return

        self.info = self.project.cache.local.save(self.path_info)

    def remove(self):
        remove(self.path)
