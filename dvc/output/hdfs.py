from dvc.dependency.hdfs import DependencyHDFS
from dvc.exceptions import DvcException


class OutputHDFS(DependencyHDFS):
    PARAM_CACHE = 'cache'
    PARAM_METRIC = 'metric'

    def __init__(self,
                 stage,
                 path,
                 info=None,
                 remote=None,
                 cache=True,
                 metric=False):
        super(OutputHDFS, self).__init__(stage, path, info, remote=remote)
        self.use_cache = cache
        self.metric = metric
        if cache and self.project.cache.hdfs is None:
            raise DvcException("No cache location setup for \'hdfs\' outputs.")

    def dumpd(self):
        ret = super(OutputHDFS, self).dumpd()
        ret[self.PARAM_CACHE] = self.use_cache
        ret[self.PARAM_METRIC] = self.metric
        return ret

    def changed(self):
        if super(OutputHDFS, self).changed():
            return True

        if self.use_cache \
           and self.project.cache.hdfs.changed(self.path_info, self.info):
            return True

        return False

    def checkout(self):
        if not self.use_cache:
            return

        self.project.cache.hdfs.checkout(self.path_info, self.info)

    def save(self):
        super(OutputHDFS, self).save()

        if not self.use_cache:
            return

        self.info = self.project.cache.hdfs.save(self.path_info)

    def remove(self, ignore_remove=False):
        self.remote.remove(self.path_info)
