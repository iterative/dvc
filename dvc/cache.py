import os


class Cache(object):
    CACHE_DIR = 'cache'

    def __init__(self, dvc_dir):
        self.cache_dir = os.path.join(dvc_dir, self.CACHE_DIR)

    @staticmethod
    def init(dvc_dir):
        cache_dir = os.path.join(dvc_dir, Cache.CACHE_DIR)
        os.mkdir(cache_dir)
        return Cache(dvc_dir)

    def all(self):
        clist = []
        for cache in os.listdir(self.cache_dir):
            path = os.path.join(self.cache_dir, cache)
            if os.path.isfile(path):
                clist.append(path)
        return clist

    def get(self, md5):
        return os.path.join(self.cache_dir, md5)
