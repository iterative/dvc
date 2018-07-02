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
    PARAM_METRIC = 'metric'

    DoesNotExistError = OutputDoesNotExistError
    IsNotFileOrDirError = OutputIsNotFileOrDirError

    def __init__(self, stage, path, info=None, remote=None, cache=True, metric=False):
        super(OutputLOCAL, self).__init__(stage, path, info, remote=remote)
        self.use_cache = cache
        self.metric = metric

    @property
    def md5(self):
        return self.info.get(self.project.cache.local.PARAM_MD5, None)

    @property
    def cache(self):
        return self.project.cache.local.get(self.md5)

    @property
    def is_local(self):
        return self.path.startswith(self.project.root_dir)

    def dumpd(self):
        ret = super(OutputLOCAL, self).dumpd()
        ret[self.PARAM_CACHE] = self.use_cache
        ret[self.PARAM_METRIC] = self.metric
        return ret

    def changed(self):
        if not self.use_cache:
            return super(OutputLOCAL, self).changed()

        return self.info != self.project.cache.local.save_info(self.path_info)

    def checkout(self):
        if not self.use_cache:
            return

        if not self.changed():
            msg = u'Data file \'{}\' didn\'t change, skipping checkout.'
            self.project.logger.debug(msg.format(self.rel_path))
            return

        self.project.cache.local.checkout(self.path_info, self.info)

    def save(self):
        if not self.use_cache:
            super(OutputLOCAL, self).save()
            msg = 'Output \'{}\' doesn\'t use cache. Skipping saving.'
            self.project.logger.debug(msg.format(self.rel_path))
            return

        if not os.path.exists(self.path):
            raise self.DoesNotExistError(self.rel_path)

        if not os.path.isfile(self.path) and not os.path.isdir(self.path):
            raise self.NotFileOrDirError(self.rel_path)

        if (os.path.isfile(self.path) and os.path.getsize(self.path) == 0) or \
           (os.path.isdir(self.path) and len(os.listdir(self.path)) == 0):
            self.project.logger.warn("File/directory '{}' is empty.".format(self.rel_path))

        if not self.changed():
            msg = 'Output \'{}\' didn\'t change. Skipping saving.'
            self.project.logger.debug(msg.format(self.rel_path))
            return

        if self.is_local:
            if self.project.scm.is_tracked(self.path):
                raise OutputAlreadyTrackedError(self.rel_path)

            if self.use_cache:
                self.project.scm.ignore(self.path)

        self.info = self.project.cache.local.save(self.path_info)

    def remove(self, ignore_remove=False):
        self.remote.remove(self.path_info)
        if ignore_remove and self.use_cache and self.is_local:
            self.project.scm.ignore_remove(self.path)

    def move(self, out):
        if self.use_cache and self.is_local:
            self.project.scm.ignore_remove(self.path)

        self.remote.move(self.path_info, out.path_info)
        self.path = out.path
        self.path_info = out.path_info
        self.save()

        if self.use_cache and self.is_local:
            self.project.scm.ignore(self.path)
