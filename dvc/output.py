import os
import stat
import json
import shutil
import schema
from checksumdir import dirhash

from dvc.system import System
from dvc.utils import file_md5
from dvc.exceptions import DvcException
from dvc.logger import Logger


class OutputError(DvcException):
    pass


class CmdOutputError(DvcException):
    def __init__(self, path, msg):
        super(CmdOutputError, self).__init__('Output file \'{}\' error: {}'.format(path, msg))


class CmdOutputNoCacheError(CmdOutputError):
    def __init__(self, path):
        super(CmdOutputNoCacheError, self).__init__(path, 'no cache')


class CmdOutputOutsideOfRepoError(CmdOutputError):
    def __init__(self, path):
        super(CmdOutputOutsideOfRepoError, self).__init__(path, 'outside of repository')


class CmdOutputDoesNotExistError(CmdOutputError):
    def __init__(self, path):
        super(CmdOutputDoesNotExistError, self).__init__(path, 'does not exist')


class CmdOutputIsNotFileOrDirError(CmdOutputError):
    def __init__(self, path):
        super(CmdOutputIsNotFileOrDirError, self).__init__(path, 'not a file or directory')


class CmdOutputAlreadyTrackedError(CmdOutputError):
    def __init__(self, path):
        super(CmdOutputAlreadyTrackedError, self).__init__(path, 'already tracked by scm(e.g. git)')


class Dependency(object):
    PARAM_RELPATH = 'relpath'
    PARAM_PATH = 'path'
    PARAM_MD5 = 'md5'
    MD5_DIR_SUFFIX = '.dir'

    SCHEMA = {
        PARAM_PATH: str,
        schema.Optional(PARAM_MD5): schema.Or(str, None),
    }

    def __init__(self, project, path, md5=None):
        self.project = project
        self.path = os.path.abspath(os.path.realpath(path))

        if not self.path.startswith(self.project.root_dir):
            raise CmdOutputOutsideOfRepoError(self.rel_path)

        self.md5 = md5

    @property
    def rel_path(self):
        return os.path.relpath(self.path)

    def _changed_md5(self):
        if not os.path.exists(self.path):
            return True

        return self.project.state.changed(self.path, self.md5)

    def changed(self):
        return self._changed_md5()

    def status(self):
        if self.changed():
            #FIXME better msgs
            return {self.rel_path: 'changed'}
        return {}

    @staticmethod
    def is_dir_cache(cache):
        return cache.endswith(Output.MD5_DIR_SUFFIX)

    def save(self):
        if not os.path.exists(self.path):
            raise CmdOutputDoesNotExistError(self.rel_path)

        if not os.path.isfile(self.path) and not os.path.isdir(self.path):
            raise CmdOutputIsNotFileOrDirError(self.rel_path)

        self.md5 = self.project.state.update(self.path)

    def dumpd(self, cwd):
        return {
            Output.PARAM_PATH: os.path.relpath(self.path, cwd),
            Output.PARAM_MD5: self.md5,
        }

    @classmethod
    def loadd(cls, project, d, cwd=os.curdir):
        path = os.path.join(cwd, d[Output.PARAM_PATH])
        md5 = d.get(Output.PARAM_MD5, None)
        return cls(project, path, md5=md5)

    @classmethod
    def loadd_from(cls, project, d_list, cwd=os.curdir):
        return [cls.loadd(project, x, cwd=cwd) for x in d_list]

    @classmethod
    def loads(cls, project, s, cwd=os.curdir):
        return cls(project, os.path.join(cwd, s), md5=None)

    @classmethod
    def loads_from(cls, project, s_list, cwd=os.curdir):
        return [cls.loads(project, x, cwd=cwd) for x in s_list]

    def stage(self):
        for stage in self.project.stages():
            for out in stage.outs:
                if self.path == out.path:
                    return stage
        return None


class Output(Dependency):
    PARAM_CACHE = 'cache'

    SCHEMA = Dependency.SCHEMA
    SCHEMA[schema.Optional(PARAM_CACHE)] = bool

    def __init__(self, project, path, md5=None, use_cache=True):
        super(Output, self).__init__(project, path, md5=md5)
        self.use_cache = use_cache

    @property
    def cache(self):
        if not self.md5:
            return None

        return self.project.cache.get(self.md5)

    @property
    def rel_cache(self):
        if not self.cache:
            return None

        return os.path.relpath(self.cache)

    def dumpd(self, cwd):
        ret = super(Output, self).dumpd(cwd)
        ret[Output.PARAM_CACHE] = self.use_cache
        return ret

    @classmethod
    def loadd(cls, project, d, cwd=os.curdir):
        ret = super(Output, cls).loadd(project, d, cwd=cwd)
        ret.use_cache = d.get(Output.PARAM_CACHE, True)
        return ret

    @classmethod
    def loads(cls, project, s, use_cache=True, cwd=os.curdir):
        ret = super(Output, cls).loads(project, s, cwd=cwd)
        ret.use_cache = use_cache
        return ret

    @classmethod
    def loads_from(cls, project, s_list, use_cache=False, cwd=os.curdir):
        return [cls.loads(project, x, use_cache=use_cache, cwd=cwd) for x in s_list]

    def _changed_cache(self, cache):
        md5 = self.project.state.update(cache)
        if md5 != self.project.cache.path_to_md5(cache):
            self.project.logger.warn('Corrupted cache file {}'.format(os.path.relpath(cache)))
            os.unlink(cache)
            return True

        return False

    def _changed_file(self, path, cache):
        if os.path.isfile(path) and \
           os.path.isfile(cache) and \
           System.samefile(path, cache) and \
           not self._changed_cache(cache):
            return False

        return True

    def _changed_dir(self):
        if not os.path.isdir(self.path) or not os.path.isfile(self.cache):
            return True

        dir_info = self._collect_dir() # slow!
        dir_info_cached = self.load_dir_cache(self.cache) # slow. why?

        if not self.are_dir_info_equal(dir_info, dir_info_cached):
            return True

        return False

    @staticmethod
    def are_dir_info_equal(dir_info1, dir_info2):
        return Output.dir_info_dict(dir_info1) == Output.dir_info_dict(dir_info2)

    @staticmethod
    def dir_info_dict(dir_info):
        return {i['relpath']: i['md5'] for i in dir_info}

    def changed(self):
        if not self.use_cache:
            ret = super(Output, self).changed()
        elif not self.cache:
            ret = True
        elif self.is_dir_cache(self.cache):
            ret = self._changed_dir()
        else:
            ret = self._changed_file(self.path, self.cache)

        msg = u'Data file or dir \'{}\' with cache \'{}\' '
        if ret:
            msg += 'changed'
        else:
            msg += 'didn\'t change'
        self.project.logger.debug(msg.format(self.rel_path, self.rel_cache))

        return ret

    def hardlink(self, src, link):
        rel_src = os.path.relpath(src)
        rel_link = os.path.relpath(link)
        self.project.logger.debug(u'creating hardlink {} -> {}'.format(rel_src, rel_link))

        dname = os.path.dirname(link)
        if not os.path.exists(dname):
            os.makedirs(dname)

        System.hardlink(src, link)

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
        d = Output.load_dir_cache(path)

        for entry in d:
            res[entry[Output.PARAM_RELPATH]] = entry[Output.PARAM_MD5]

        return res

    def dir_cache(self):
        res = {}
        dir_cache = self.get_dir_cache(self.cache)

        for relpath, md5 in dir_cache.items():
            res[relpath] = self.project.cache.get(md5)

        return res

    def checkout(self):
        if not self.use_cache:
            return

        msg = u'Checking out \'{}\' with cache \'{}\''
        self.project.logger.debug(msg.format(self.rel_path, self.rel_cache))

        if not self.changed():
            msg = u'Data file \'{}\' with cache \'{}\' didn\'t change, skipping checkout.'
            self.project.logger.debug(msg.format(self.rel_path, self.rel_cache))
            return

        if not self.cache or not os.path.exists(self.cache):
            if self.cache:
                self.project.logger.warn(u'\'{}({})\': cache file not found'.format(self.rel_cache, self.rel_path))
            self.remove()
            return

        if os.path.exists(self.path):
            msg = u'Data file \'{}\' exists. Removing before checkout'
            self.project.logger.debug(msg.format(self.rel_path))
            self.remove()

        if not self.is_dir_cache(self.cache):
            self.hardlink(self.cache, self.path)
            return

        for relpath, cache in self.dir_cache().items():
            path = os.path.join(self.path, relpath)
            self.hardlink(cache, path)

    def _collect_dir(self):
        dir_info = []

        for root, dirs, files in os.walk(self.path):
            for fname in files:
                path = os.path.join(root, fname)
                relpath = os.path.relpath(path, self.path)

                md5 = self.project.state.update(path, dump=False)
                dir_info.append({self.PARAM_RELPATH: relpath, self.PARAM_MD5: md5})

        self.project.state.dump()

        return dir_info

    def _save_dir(self):
        dname = os.path.dirname(self.cache)
        dir_info = self._collect_dir()

        for entry in dir_info:
            md5 = entry[self.PARAM_MD5]
            relpath = entry[self.PARAM_RELPATH]
            path = os.path.join(self.path, relpath)
            cache = self.project.cache.get(md5)

            if os.path.exists(cache):
                self._remove(path)
                self.hardlink(cache, path)
            else:
                self.hardlink(path, cache)

        if not os.path.isdir(dname):
            os.makedirs(dname)

        with open(self.cache, 'w+') as fd:
            json.dump(dir_info, fd)

    def save(self):
        super(Output, self).save()

        if not self.use_cache:
            return

        self.project.logger.debug(u'Saving \'{}\' to \'{}\''.format(self.rel_path, self.rel_cache))

        if self.project.scm.is_tracked(self.path):
            raise CmdOutputAlreadyTrackedError(self.rel_path)

        if not self.changed():
             return

        if os.path.exists(self.cache):
            # This means that we already have cache for this data.
            # We remove data and link it to existing cache to save
            # some space.
            msg = u'Cache \'{}\' already exists, performing checkout for \'{}\''
            self.project.logger.debug(msg.format(self.rel_cache, self.rel_path))
            self.checkout()
            return

        if os.path.isdir(self.path):
            self._save_dir()
        else:
            self.hardlink(self.path, self.cache)

    def _remove(self, path):
        if not os.path.exists(path):
            return

        self.project.logger.debug(u'Removing \'{}\''.format(os.path.relpath(path)))
        if os.path.isfile(path):
            os.unlink(path)
        else:
            shutil.rmtree(path)

    def remove(self):
        self._remove(self.path)
