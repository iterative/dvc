from dvc.dependency.gs import DependencyGS


class OutputGS(DependencyGS):
    PARAM_CACHE = 'cache'

    def __init__(self, stage, path, info=None, cache=True):
        super(OutputGS, self).__init__(stage, path, info)
        self.use_cache = cache

    def dumpd(self):
        ret = super(OutputGS, self).dumpd()
        ret[self.PARAM_CACHE] = self.use_cache
        return ret

    def changed(self):
        if super(OutputGS, self).changed():
            return True

        if self.use_cache and self.info != self.project.cache.gs.save_info(self.path_info):
            return True

        return False

    def checkout(self):
        if not self.use_cache:
            return

        self.project.cache.gs.checkout(self.path_info, self.info)

    def save(self):
        super(OutputGS, self).save()

        if not self.use_cache:
            return


        self.info = self.project.cache.gs.save(self.path_info)

    def remove(self):
        blob = self.client.bucket(self.bucket).get_blob(self.key)
        if not blob:
            return

        blob.delete()
