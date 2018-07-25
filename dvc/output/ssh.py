from dvc.dependency.ssh import DependencySSH
from dvc.exceptions import DvcException


class OutputSSH(DependencySSH):
    PARAM_CACHE = 'cache'
    PARAM_METRIC = 'metric'

    def __init__(self,
                 stage,
                 path,
                 info=None,
                 remote=None,
                 cache=True,
                 metric=False):
        super(OutputSSH, self).__init__(stage, path, info, remote=remote)
        self.use_cache = cache
        self.metric = metric
        if cache and self.project.cache.ssh is None:
            raise DvcException("No cache location setup for \'ssh\' outputs.")

    def dumpd(self):
        ret = super(OutputSSH, self).dumpd()
        ret[self.PARAM_CACHE] = self.use_cache
        ret[self.PARAM_METRIC] = self.metric
        return ret

    def changed(self):
        if super(OutputSSH, self).changed():
            return True

        if self.use_cache \
           and self.info != self.project.cache.ssh.save_info(self.path_info):
            return True

        return False

    def checkout(self):
        if not self.use_cache:
            return

        self.project.cache.ssh.checkout(self.path_info, self.info)

    def save(self):
        super(OutputSSH, self).save()

        if not self.use_cache:
            return

        self.info = self.project.cache.ssh.save(self.path_info)

    def remove(self, ignore_remove=False):
        self.remote.remove(self.path_info)
