import os
import stat
import yaml
from checksumdir import dirhash

from dvc.system import System
from dvc.utils import file_md5
from dvc.exceptions import DvcException


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
    PARAM_PATH = 'path'
    PARAM_MD5 = 'md5'

    def __init__(self, project, path, md5=None):
        self.project = project
        self.path = os.path.abspath(os.path.realpath(path))

        if not self.path.startswith(self.project.root_dir):
            raise CmdOutputOutsideOfRepoError(self.rel_path)

        self.md5 = md5

    @property
    def dvc_path(self):
        return os.path.relpath(self.path, self.project.root_dir)

    @property
    def rel_path(self):
        return os.path.relpath(self.path, '.')

    def _changed_md5(self):
        if not os.path.exists(self.path):
            return True

        state = self.project.state.get(self.path)
        if state and state.mtime == self.mtime():
            md5 = state.md5
        else:
            md5 = self.compute_md5()

        return self.md5 != md5

    def changed(self):
        return self._changed_md5()

    def compute_md5(self):
        if os.path.isdir(self.path):
            return dirhash(self.path, hashfunc='md5')
        else:
            return file_md5(self.path)[0]

    def mtime(self):
        return os.path.getmtime(self.path)

    def inode(self):
        return os.stat(self.path).st_ino

    def save(self):
        if not os.path.exists(self.path):
            raise CmdOutputDoesNotExistError(self.rel_path)

        if not os.path.isfile(self.path) and not os.path.isdir(self.path):
            raise CmdOutputIsNotFileOrDirError(self.rel_path)

        state = self.project.state.get(self.path)
        if state and state.mtime == self.mtime() and state.inode == self.inode():
            md5 = state.md5
            msg = '{} using md5 {} from state file'
            self.project.logger.debug(msg.format(self.path, md5))
            self.md5 = md5
        else:
            self.md5 = self.compute_md5()
            self.project.state.update(self.path, self.md5, self.mtime(), self.inode())

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

    def __init__(self, project, path, md5=None, use_cache=True):
        super(Output, self).__init__(project, path, md5=md5)
        self.use_cache = use_cache

    @property
    def cache(self):
        return self.project.cache.get(self.md5)

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

    def changed(self):
        ret = True

        if not self.use_cache:
            ret = super(Output, self).changed()
        elif os.path.exists(self.path) and \
           os.path.exists(self.cache) and \
           System.samefile(self.path, self.cache) and \
           os.stat(self.cache).st_mode & stat.S_IREAD:
            ret = False

        msg = "Data {} with cache {} "
        if ret:
            msg += "changed"
        else:
            msg += "didn't change"
        self.project.logger.debug(msg.format(self.path, self.cache))

        return ret

    def hardlink(self, src, link):
        self.project.logger.debug("creating hardlink {} -> {}".format(src, link))
        System.hardlink(src, link)
        os.chmod(src, stat.S_IREAD)

    def dir_cache(self):
        res = {}
        for root, dirs, files in os.walk(self.cache):
            for fname in files:
                path = os.path.join(root, fname)
                relpath = os.path.relpath(path, self.cache)
                with open(path, 'r') as fd:
                    d = yaml.safe_load(fd)
                md5 = d[Output.PARAM_MD5]
                res[relpath] = self.project.cache.get(md5)
        return res

    def checkout(self):
        if not self.use_cache:
            return

        self.project.logger.debug("Checking out {} with cache {}".format(self.path, self.cache))

        if not self.changed():
            msg = "Data {} with cache {} didn't change, skipping checkout."
            self.project.logger.debug(msg.format(self.path, self.cache))
            return

        if not os.path.exists(self.cache):
            self.project.logger.warn(u'\'{}\': cache file not found'.format(self.dvc_path))
            self.remove()
            return

        if os.path.exists(self.path):
            msg = "Data {} exists. Removing before checkout"
            self.project.logger.debug(msg.format(self.path))
            self.remove()

        if os.path.isfile(self.cache):
            self.hardlink(self.cache, self.path)
            return

        for relpath, cache in self.dir_cache().items():
            path = os.path.join(self.path, relpath)
            dname = os.path.dirname(path)

            if not os.path.exists(dname):
                os.makedirs(dname)

            self.hardlink(cache, path)

    def save(self):
        super(Output, self).save()

        if not self.use_cache:
            return

        self.project.logger.debug("Saving {} to {}".format(self.path, self.cache))

        if self.project.scm.is_tracked(self.path):
            raise CmdOutputAlreadyTrackedError(self.rel_path)

        if not self.changed():
             return

        if os.path.exists(self.cache):
            # This means that we already have cache for this data.
            # We remove data and link it to existing cache to save
            # some space.
            msg = "Cache {} already exists, performing checkout for {}"
            self.project.logger.debug(msg.format(self.cache, self.path))
            self.checkout()
            return

        if os.path.isfile(self.path):
            self.hardlink(self.path, self.cache)
            return

        for root, dirs, files in os.walk(self.path):
            for fname in files:
                path = os.path.join(root, fname)
                relpath = os.path.relpath(path, self.path)
                md5 = file_md5(path)[0]
                cache = self.project.cache.get(md5)
                cache_info = os.path.join(self.cache, relpath)
                cache_dir = os.path.dirname(cache_info)

                if os.path.exists(cache):
                    self._remove(path, None)
                    self.hardlink(cache, path)
                else:
                    self.hardlink(path, cache)

                if not os.path.exists(cache_dir):
                    os.makedirs(cache_dir)

                with open(cache_info, 'w') as fd:
                    yaml.safe_dump({self.PARAM_MD5: md5}, fd, default_flow_style=False)

    def _remove(self, path, cache):
        self.project.logger.debug("Removing '{}'".format(path))
        os.chmod(path, stat.S_IWUSR)
        os.unlink(path)
        if cache != None and os.path.exists(cache):
            os.chmod(cache, stat.S_IREAD)

    def remove(self):
        if not os.path.exists(self.path):
            return

        if os.path.isfile(self.path):
            self._remove(self.path, self.cache)
            return

        caches = self.dir_cache()
        for root, dirs, files in os.walk(self.path, topdown=False):
            for d in dirs:
                path = os.path.join(root, d)
                os.rmdir(path)
            for f in files:
                path = os.path.join(root, f)
                relpath = os.path.relpath(path, self.path)
                cache = caches.get(relpath, None)
                self._remove(path, cache)
        os.rmdir(self.path)
