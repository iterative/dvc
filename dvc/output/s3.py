from dvc.dependency.s3 import DependencyS3
from dvc.exceptions import DvcException


class OutputS3(DependencyS3):
    PARAM_CACHE = 'cache'
    PARAM_METRIC = 'metric'

    def __init__(self,
                 stage,
                 path,
                 info=None,
                 remote=None,
                 cache=True,
                 metric=False):
        super(OutputS3, self).__init__(stage, path, info, remote=remote)
        self.use_cache = cache
        self.metric = metric
        if cache and self.project.cache.s3 is None:
            raise DvcException("No cache location setup for \'s3\' outputs.")

    def dumpd(self):
        ret = super(OutputS3, self).dumpd()
        ret[self.PARAM_CACHE] = self.use_cache
        ret[self.PARAM_METRIC] = self.metric
        return ret

    def changed(self):
        if super(OutputS3, self).changed():
            return True

        if self.use_cache \
           and self.info != self.project.cache.s3.save_info(self.path_info):
            return True

        return False

    def checkout(self):
        if not self.use_cache:
            return

        self.project.cache.s3.checkout(self.path_info, self.info)

    def save(self):
        super(OutputS3, self).save()

        if not self.use_cache:
            return

        self.info = self.project.cache.s3.save(self.path_info)

    def remove(self, ignore_remove=False):
        self.remote.remove(self.path_info)
