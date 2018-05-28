from dvc.dependency.s3 import DependencyS3


class OutputS3(DependencyS3):
    PARAM_CACHE = 'cache'

    def __init__(self, stage, path, etag=None, cache=True):
        super(OutputS3, self).__init__(stage, path, etag=etag)
        self.use_cache = cache

    @property
    def cache(self):
        if not self.use_cache:
            return None

        raise NotImplementedError

        return self.project.cache.get(self.etag)

    def dumpd(self):
        ret = super(OutputS3, self).dumpd()
        ret[self.PARAM_CACHE] = self.use_cache
        return ret

    def changed(self):
        if super(OutputS3, self).changed():
            return True

        if self.use_cache and self.project.cache.changed(self.etag):
            return True

        return False

    def checkout(self):
        if not self.use_cache:
            return

        raise NotImplementedError

    def save(self):
        super(OutputS3, self).save()

        if not self.use_cache:
            return

        raise NotImplementedError

    def remove(self):
        self.s3.Object(self.bucket, self.key).delete()
