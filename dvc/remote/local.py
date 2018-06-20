import os
import uuid
import json
import shutil
import filecmp

from dvc.system import System
from dvc.remote.base import RemoteBase
from dvc.state import State
from dvc.logger import Logger
from dvc.utils import remove, move, copyfile, file_md5
from dvc.config import Config


class LocalKey(object):
    def __init__(self, bucket, name):
        self.name = name
        self.bucket = bucket

    @property
    def path(self):
        return os.path.join(self.bucket, self.name)


class RemoteLOCAL(RemoteBase):
    scheme = ''
    REGEX = r'^(?P<path>(/+|.:\\+).*)$'
    PARAM_MD5 = State.PARAM_MD5

    CACHE_TYPES = ['reflink', 'hardlink', 'symlink', 'copy']
    CACHE_TYPE_MAP = {
        'copy': shutil.copyfile,
        'symlink': System.symlink,
        'hardlink': System.hardlink,
        'reflink': System.reflink,
    }

    def __init__(self, project, config):
        self.project = project
        self.link_state = project.link_state
        storagepath = config.get(Config.SECTION_AWS_STORAGEPATH, None)
        self.cache_dir = config.get(Config.SECTION_REMOTE_URL, storagepath)
        self.cache_type = config.get(Config.SECTION_CACHE_TYPE, None)

        if self.cache_dir != None and not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir)

        self.state = State(self.cache_dir)

    @property
    def prefix(self):
        return self.cache_dir

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
        if not md5:
            return None

        return os.path.join(self.cache_dir, md5[0:2], md5[2:])

    def path_to_md5(self, path):
        relpath = os.path.relpath(path, self.cache_dir)
        return os.path.dirname(relpath) + os.path.basename(relpath)

    def changed(self, md5):
        cache = self.get(md5)
        if self.state.changed(cache, md5=md5):
            if os.path.exists(cache):
                Logger.warn('Corrupted cache file {}'.format(os.path.relpath(cache)))
                remove(cache)
            return True

        return False

    def link(self, src, link, dump=True):
        dname = os.path.dirname(link)
        if not os.path.exists(dname):
            os.makedirs(dname)

        if self.cache_type != None:
            types = [self.cache_type]
        else:
            types = self.CACHE_TYPES

        for typ in types:
            try:
                msg = u'Checking out \'{}\' with cache \'{}\''
                Logger.debug(msg.format(os.path.relpath(src), os.path.relpath(link)))
                self.CACHE_TYPE_MAP[typ](src, link)
                self.link_state.update(link, dump=dump)
                return
            except Exception as exc:
                msg = 'Cache type \'{}\' is not supported'.format(typ)
                Logger.debug(msg)
                if typ == types[-1]:
                    raise
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

    @classmethod
    def get_dir_cache(cls, path):
        res = {}

        d = cls.load_dir_cache(path)

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

    def checkout(self, path_info, checksum_info, dump=True):
        path = path_info['path']
        md5 = checksum_info.get(self.PARAM_MD5, None)
        cache = self.get(md5)

        if not cache or not os.path.exists(cache) or self.changed(md5):
            if cache:
                Logger.warn(u'\'{}({})\': cache file not found'.format(os.path.relpath(cache),
                                                                       os.path.relpath(path)))
            remove(path)
            return

        if os.path.exists(path):
            msg = u'Data \'{}\' exists. Removing before checkout'
            Logger.debug(msg.format(os.path.relpath(path)))
            remove(path)

        if not self.is_dir_cache(cache):
            self.link(cache, path, dump=dump)
            return

        msg = u'Checking out directory \'{}\' with cache \'{}\''
        Logger.debug(msg.format(os.path.relpath(path), os.path.relpath(cache)))

        # Create dir separately so that dir is created
        # even if there are no files in it
        if not os.path.exists(path):
            os.makedirs(path)

        dir_cache = self.dir_cache(cache)
        for relpath, c in dir_cache.items():
            p = os.path.join(path, relpath)
            self.link(c, p, dump=dump)

    def _move(self, inp, outp):
        # moving in two stages to make last the move atomic in
        # case inp and outp are in different filesystems
        tmp = '{}.{}'.format(outp, str(uuid.uuid4()))
        move(inp, tmp)
        move(tmp, outp)

    def _save_file(self, path_info, dump=True):
        path = path_info['path']
        md5 = self.state.update(path)
        cache = self.get(md5)
        if self.changed(md5):
            Logger.debug(u'Saving \'{}\' to \'{}\''.format(os.path.relpath(path),
                                                           os.path.relpath(cache)))
            self._move(path, cache)
            self.state.update(cache)
        self.checkout(path_info, {self.PARAM_MD5: md5}, dump=dump)

        return {self.PARAM_MD5: md5}

    def _save_dir(self, path_info):
        path = path_info['path']
        md5 = self.state.update(path)
        cache = self.get(md5)
        dname = os.path.dirname(cache)
        dir_info = self.state.collect_dir(path)

        for entry in dir_info:
            relpath = entry[State.PARAM_RELPATH]
            p = os.path.join(path, relpath)

            self._save_file({'scheme': 'local', 'path': p}, dump=False)
        self.link_state.dump()

        if not os.path.isdir(dname):
            os.makedirs(dname)

        Logger.debug(u'Saving directory \'{}\' to \'{}\''.format(os.path.relpath(path),
                                                                 os.path.relpath(cache)))

        with open(cache, 'w+') as fd:
            json.dump(dir_info, fd, sort_keys=True)

        return {self.PARAM_MD5: md5}

    def save(self, path_info):
        if path_info['scheme'] != 'local':
            raise NotImplementedError

        path = path_info['path']

        if os.path.isdir(path):
            return self._save_dir(path_info)
        else:
            return self._save_file(path_info)

    def save_info(self, path_info):
        if path_info['scheme'] != 'local':
            raise NotImplementedError

        return {self.PARAM_MD5: self.state.update(path_info['path'])}

    def remove(self, path_info):
        if path_info['scheme'] != 'local':
            raise NotImplementedError

        remove(path_info['path'])

    def _copy(self, from_path, to_path):
        dname = os.path.dirname(to_path)
        if not os.path.exists(dname):
            os.makedirs(dname)

        shutil.copyfile(from_path, to_path)

    def upload(self, path, path_info):
        if path_info['scheme'] != 'local':
            raise NotImplementedError

        self._copy(path, path_info['path'])

    def download(self, path_info, path):
        if path_info['scheme'] != 'local':
            raise NotImplementedError

        self._copy(path_info['path'], path)

    # old code starting from here
    def cache_file_key(self, path):
        return os.path.relpath(os.path.abspath(path), self.project.cache.local.cache_dir)

    def _get_key(self, path):
        key_name = self.cache_file_key(path)
        key = LocalKey(self.prefix, key_name)
        if os.path.exists(key.path) and os.path.isfile(key.path):
            return key
        return None

    def _new_key(self, path):
        key_name = self.cache_file_key(path)
        key = LocalKey(self.prefix, key_name)
        self._makedirs(key.path)
        return key

    def _push_key(self, key, path):
        self._makedirs(key.path)
        name = self.cache_key_name(path)
        copyfile(path, key.path, name=name)
        return path

    def _pull_key(self, key, path, no_progress_bar=False):
        self._makedirs(path)

        name = self.cache_key_name(path)

        tmp_file = self.tmp_file(path)
        try:
            copyfile(key.path, tmp_file, no_progress_bar=no_progress_bar, name=name)
        except Exception as exc:
            Logger.error('Failed to copy "{}": {}'.format(key.path, exc))
            return None

        os.rename(tmp_file, path)

        return path
