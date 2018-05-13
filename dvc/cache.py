import os
import json
import shutil

from dvc.state import State, LinkState
from dvc.system import System
from dvc.logger import Logger
from dvc.utils import move, remove
from dvc.lock import Lock
from dvc.exceptions import DvcException


class Cache(object):
    CACHE_DIR = 'cache'
    CACHE_DIR_LOCK = 'cache.lock'
    CACHE_TYPES = ['reflink', 'hardlink', 'symlink', 'copy']
    CACHE_TYPE_MAP = {
        'copy': shutil.copyfile,
        'symlink': System.symlink,
        'hardlink': System.hardlink,
        'reflink': System.reflink,
    }

    def __init__(self, root_dir, dvc_dir, cache_dir=None, cache_type=None):
        self.cache_type = cache_type

        cache_dir = cache_dir if cache_dir else self.CACHE_DIR
        if os.path.isabs(cache_dir):
            self.cache_dir = cache_dir
        else:
            self.cache_dir = os.path.abspath(os.path.realpath(os.path.join(dvc_dir, cache_dir)))

        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir)

        self.state = State(self.cache_dir)
        self.link_state = LinkState(root_dir, dvc_dir)
        self.lock = Lock(self.cache_dir, name=self.CACHE_DIR_LOCK)

    @staticmethod
    def init(root_dir, dvc_dir, cache_dir=None):
        return Cache(root_dir, dvc_dir, cache_dir=None)

    def all(self):
        with self.lock:
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
        if not md5:
            return None

        return os.path.join(self.cache_dir, md5[0:2], md5[2:])

    def path_to_md5(self, path):
        relpath = os.path.relpath(path, self.cache_dir)
        return os.path.dirname(relpath) + os.path.basename(relpath)

    def _changed(self, md5):
        cache = self.get(md5)
        if self.state.changed(cache, md5=md5):
            if os.path.exists(cache):
                Logger.warn('Corrupted cache file {}'.format(os.path.relpath(cache)))
                remove(cache)
            return True

        return False

    def changed(self, md5):
        with self.lock:
            return self._changed(md5)

    def link(self, src, link):
        dname = os.path.dirname(link)
        if not os.path.exists(dname):
            os.makedirs(dname)

        if self.cache_type != None:
            types = [self.cache_type]
        else:
            types = self.CACHE_TYPES

        for typ in types:
            try:
                self.CACHE_TYPE_MAP[typ](src, link)
                self.link_state.update(link)
                return
            except Exception as exc:
                msg = 'Cache type \'{}\' is not supported'.format(typ)
                Logger.debug(msg)
                if typ == types[-1]:
                    raise DvcException(msg, cause=exc)

    @staticmethod
    def load_dir_cache(path):
        if os.path.isabs(path):
            relpath = os.path.relpath(path)
        else:
            relpath = path

        try:
            with open(path, 'r') as fd:
                d = json.load(fd)
        except Exception as exc:
            msg = u'Failed to load dir cache \'{}\''
            Logger.error(msg.format(relpath), exc)
            return []

        if not isinstance(d, list):
            msg = u'Dir cache file format error \'{}\': skipping the file'
            Logger.error(msg.format(relpath))
            return []

        return d

    @staticmethod
    def get_dir_cache(path):
        res = {}
        d = Cache.load_dir_cache(path)

        for entry in d:
            res[entry[State.PARAM_RELPATH]] = entry[State.PARAM_MD5]

        return res

    def dir_cache(self, cache):
        res = {}
        dir_cache = self.get_dir_cache(cache)

        for relpath, md5 in dir_cache.items():
            res[relpath] = self.get(md5)

        return res

    @staticmethod
    def is_dir_cache(cache):
        return cache.endswith(State.MD5_DIR_SUFFIX)

    def _checkout(self, path, md5):
        cache = self.get(md5)

        if not cache or not os.path.exists(cache) or self._changed(md5):
            if cache:
                Logger.warn(u'\'{}({})\': cache file not found'.format(os.path.relpath(cache),
                                                                       os.path.relpath(path)))
            remove(path)
            return

        if os.path.exists(path):
            msg = u'Data \'{}\' exists. Removing before checkout'
            Logger.debug(msg.format(os.path.relpath(path)))
            remove(path)

        msg = u'Checking out \'{}\' with cache \'{}\''
        Logger.debug(msg.format(os.path.relpath(path), os.path.relpath(cache)))

        if not self.is_dir_cache(cache):
            self.link(cache, path)
            return

        dir_cache = self.dir_cache(cache)
        for relpath, c in dir_cache.items():
            p = os.path.join(path, relpath)
            self.link(c, p)

    def checkout(self, path, md5):
        with self.lock:
            return self._checkout(path, md5)

    def _save_file(self, path):
        md5 = self.state.update(path)
        cache = self.get(md5)
        if self._changed(md5):
            move(path, cache)
            self.state.update(cache)
        self._checkout(path, md5)

    def _save_dir(self, path):
        md5 = self.state.update(path)
        cache = self.get(md5)
        dname = os.path.dirname(cache)
        dir_info = self.state.collect_dir(path)

        for entry in dir_info:
            relpath = entry[State.PARAM_RELPATH]
            p = os.path.join(path, relpath)

            self._save_file(p)

        if not os.path.isdir(dname):
            os.makedirs(dname)

        with open(cache, 'w+') as fd:
            json.dump(dir_info, fd, sort_keys=True)

    def save(self, path):
        with self.lock:
            if os.path.isdir(path):
                self._save_dir(path)
            else:
                self._save_file(path)
