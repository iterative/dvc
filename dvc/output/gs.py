from dvc.dependency.gs import DependencyGS


class OutputGS(DependencyGS):
    PARAM_CACHE = 'cache'

    def __init__(self, stage, path, etag=None, use_cache=True):
        super(OutputGS, self).__init__(stage, path, etag=etag)
        self.use_cache = use_cache

    @property
    def cache(self):
        if not self.use_cache:
            return None

        raise NotImplementedError

        return self.project.cache.get(self.etag)

    def dumpd(self):
        ret = super(OutputGS, self).dumpd()
        ret[self.PARAM_CACHE] = self.use_cache
        return ret

    @classmethod
    def loadd(cls, stage, d):
        ret = super(OutputGS, cls).loadd(stage, d)
        ret.use_cache = d.get(cls.PARAM_CACHE, True)
        return ret

    def changed(self):
        if super(OutputGS, self).changed():
            return True

        if self.use_cache and self.project.cache.changed(self.etag):
            return True

        return False

    def checkout(self):
        if not self.use_cache:
            return

        raise NotImplementedError

    def save(self):
        super(OutputGS, self).save()

        if not self.use_cache:
            return

        raise NotImplementedError

    def remove(self):
        blob = self.client.bucket(self.bucket).get_blob(self.key)
        if not blob:
            return

        blob.delete()
