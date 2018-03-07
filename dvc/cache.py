import os

from dvc.system import System


class Cache(object):
    CACHE_DIR = 'cache'

    def __init__(self, dvc_dir):
        self.cache_dir = os.path.abspath(os.path.realpath(os.path.join(dvc_dir, self.CACHE_DIR)))
        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir)

    @staticmethod
    def init(dvc_dir):
        cache_dir = os.path.join(dvc_dir, Cache.CACHE_DIR)
        os.mkdir(cache_dir)
        return Cache(dvc_dir)

    def all(self):
        clist = []
        for entry in os.listdir(self.cache_dir):
            subdir = os.path.join(self.cache_dir, entry)
            if not os.path.isdir(subdir):
                continue

            for cache in os.listdir(subdir):
                path = os.path.join(subdir, cache)
                clist.append(path)

        return clist

    def get(self, md5):
        return os.path.join(self.cache_dir, md5[0:2], md5[2:])

    def path_to_md5(self, path):
        relpath = os.path.relpath(path, self.cache_dir)
        return os.path.dirname(relpath) + os.path.basename(relpath)

    def find_cache(self, files):
        file_set = set(files)
        cached = {}
        for cache_file in self.all():
            cached_files = list(filter(lambda f: System.samefile(cache_file, f), file_set))
            cached.update(dict((f, os.path.basename(os.path.dirname(cache_file)) + os.path.basename(cache_file)) for f in cached_files))
            file_set = file_set - set(cached_files)
        return cached

