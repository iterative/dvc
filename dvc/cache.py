import os

from dvc.system import System


class Cache(object):
    CACHE_DIR = 'cache'

    def __init__(self, dvc_dir):
        self.cache_dir = os.path.abspath(os.path.realpath(os.path.join(dvc_dir, self.CACHE_DIR)))

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

    def find_cache(self, files):
        file_set = set(files)
        cached = {}
        for cache_file in self.all():
            cached_files = list(filter(lambda f: System.samefile(cache_file, f), file_set))
            cached.update(dict((f, os.path.basename(cache_file)) for f in cached_files))
            file_set = file_set - set(cached_files)
        return cached
