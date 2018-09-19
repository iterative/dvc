import os
from schema import Optional, Or

from dvc.output.base import OutputDoesNotExistError, OutputIsNotFileOrDirError
from dvc.output.base import OutputAlreadyTrackedError
from dvc.dependency.local import DependencyLOCAL
from dvc.exceptions import DvcException
from dvc.istextfile import istextfile


class OutputLOCAL(DependencyLOCAL):
    PARAM_CACHE = 'cache'
    PARAM_METRIC = 'metric'
    PARAM_METRIC_TYPE = 'type'
    PARAM_METRIC_XPATH = 'xpath'

    METRIC_SCHEMA = Or(None, bool,
                       {Optional(PARAM_METRIC_TYPE): Or(str, None),
                        Optional(PARAM_METRIC_XPATH): Or(str, None)})

    DoesNotExistError = OutputDoesNotExistError
    IsNotFileOrDirError = OutputIsNotFileOrDirError

    def __init__(self,
                 stage,
                 path,
                 info=None,
                 remote=None,
                 cache=True,
                 metric=False):
        super(OutputLOCAL, self).__init__(stage, path, info, remote=remote)
        self.use_cache = cache
        self.metric = metric

    @property
    def md5(self):
        return self.info.get(self.project.cache.local.PARAM_MD5, None)

    @property
    def cache(self):
        return self.project.cache.local.get(self.md5)

    def dumpd(self):
        ret = super(OutputLOCAL, self).dumpd()
        ret[self.PARAM_CACHE] = self.use_cache

        if isinstance(self.metric, dict):
            if self.PARAM_METRIC_XPATH in self.metric and \
               not self.metric[self.PARAM_METRIC_XPATH]:
                del self.metric[self.PARAM_METRIC_XPATH]

        if self.metric:
            ret[self.PARAM_METRIC] = self.metric

        return ret

    def changed(self):
        if not self.use_cache:
            return super(OutputLOCAL, self).changed()

        return self.project.cache.local.changed(self.path_info, self.info)

    def checkout(self):
        if not self.use_cache:
            return

        self.project.cache.local.checkout(self.path_info, self.info)

    def _verify_metric(self):
        if not self.metric:
            return

        if os.path.isdir(self.path):
            msg = 'Directory \'{}\' cannot be used as metrics.'
            raise DvcException(msg.format(self.rel_path))

        if not istextfile(self.path):
            msg = 'Binary file \'{}\' cannot be used as metrics.'
            raise DvcException(msg.format(self.rel_path))

    def save(self):
        if not self.use_cache:
            super(OutputLOCAL, self).save()
            self._verify_metric()
            msg = 'Output \'{}\' doesn\'t use cache. Skipping saving.'
            self.project.logger.debug(msg.format(self.rel_path))
            return

        if not os.path.exists(self.path):
            raise self.DoesNotExistError(self.rel_path)

        if not os.path.isfile(self.path) and not os.path.isdir(self.path):
            raise self.IsNotFileOrDirError(self.rel_path)

        if (os.path.isfile(self.path) and os.path.getsize(self.path) == 0) or \
           (os.path.isdir(self.path) and len(os.listdir(self.path)) == 0):
            msg = "File/directory '{}' is empty.".format(self.rel_path)
            self.project.logger.warn(msg)

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
