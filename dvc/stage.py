import os
import stat
import yaml
import itertools
import subprocess
from checksumdir import dirhash

from dvc.system import System
from dvc.utils import file_md5
from dvc.exceptions import DvcException


class OutputError(DvcException):
    pass


class MissingDataSource(OutputError):
    def __init__(self, missing_files):
        assert len(missing_files) > 0

        source = 'source'
        if len(missing_files) > 1:
            source += 's'

        msg = u'missing data {}: {}'.format(source, ', '.join(missing_files))
        super(MissingDataSource, self).__init__(msg)


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
            raise CmdOutputOutsideOfRepoError(self.path)

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

    def update(self):
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
        if not self.use_cache:
            return super(Output, self).changed()

        if os.path.exists(self.path) and \
           os.path.exists(self.cache) and \
           System.samefile(self.path, self.cache) and \
           os.stat(self.cache).st_mode & stat.S_IREAD:
            return False

        return True

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
            msg = "Data {} with cache {} didn't change"
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
            os.makedirs(os.path.dirname(path))
            self.hardlink(cache, path)

    def save(self):
        if not self.use_cache:
            return

        self.project.logger.debug("Saving {} to {}".format(self.path, self.cache))

        if not os.path.exists(self.path):
            raise CmdOutputDoesNotExistError(self.rel_path)

        if not os.path.isfile(self.path) and not os.path.isdir(self.path):
            raise CmdOutputIsNotFileOrDirError(self.rel_path)

        if self.project.scm.is_tracked(self.path):
            raise CmdOutputAlreadyTrackedError(self.rel_path)

        if not self.changed():
            msg = "Data {} with cache {} didn't change"
            self.project.logger.debug(msg.format(self.path, self.cache))
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


class StageCmdFailedError(DvcException):
    def __init__(self, stage):
        msg = 'Stage {} cmd {} failed'.format(stage.path, stage.cmd)
        super(StageCmdFailedError, self).__init__(msg)


class Stage(object):
    STAGE_FILE = 'Dvcfile'
    STAGE_FILE_SUFFIX = '.dvc'

    PARAM_CMD = 'cmd'
    PARAM_DEPS = 'deps'
    PARAM_OUTS = 'outs'

    def __init__(self, project, path=None, cmd=None, cwd=None, deps=[], outs=[]):
        self.project = project
        self.path = path
        self.cmd = cmd
        self.cwd = cwd
        self.outs = outs
        self.deps = deps

    @property
    def relpath(self):
        return os.path.relpath(self.path)

    @property
    def dvc_path(self):
        return os.path.relpath(self.path, self.project.root_dir)

    @property
    def is_data_source(self):
        return self.cmd is None

    @staticmethod
    def is_stage_file(path):
        if not os.path.isfile(path):
            return False

        if not path.endswith(Stage.STAGE_FILE_SUFFIX) and os.path.basename(path) != Stage.STAGE_FILE:
            return False

        return True

    def changed(self):
        for entry in itertools.chain(self.outs, self.deps):
            if entry.changed():
                self.project.logger.debug("{} changed".format(self.path))
                return True
        return False

    def remove_outs(self):
        for out in self.outs:
            out.remove()
            if out.use_cache:
                self.project.scm.ignore_remove(out.path)

    def remove(self):
        self.remove_outs()
        os.unlink(self.path)

    def reproduce(self, force=False):
        if not self.changed() and not force:
            return

        if self.cmd:
            # Removing outputs only if we actually have command to reproduce
            self.remove_outs()

        self.run()

    @staticmethod
    def loadd(project, d, path):
        path = os.path.abspath(path)
        cwd = os.path.dirname(path)
        cmd = d[Stage.PARAM_CMD]
        deps = Dependency.loadd_from(project, d[Stage.PARAM_DEPS], cwd=cwd)
        outs = Output.loadd_from(project, d[Stage.PARAM_OUTS], cwd=cwd)

        return Stage(project=project,
                     path=path,
                     cmd=cmd,
                     cwd=cwd,
                     deps=deps,
                     outs=outs)
    @staticmethod
    def loads(project=None,
              cmd=None,
              deps=[],
              outs=[],
              outs_no_cache=[],
              fname=None,
              cwd=os.curdir):
        cwd = os.path.abspath(cwd)
        path = os.path.join(cwd, fname)
        outputs = Output.loads_from(project, outs, use_cache=True, cwd=cwd)
        outputs += Output.loads_from(project, outs_no_cache, use_cache=False, cwd=cwd)
        dependencies = Dependency.loads_from(project, deps, cwd=cwd)

        return Stage(project=project,
                     path=path,
                     cmd=cmd,
                     cwd=cwd,
                     outs=outputs,
                     deps=dependencies)

    @staticmethod
    def load(project, fname):
        with open(fname, 'r') as fd:
            return Stage.loadd(project, yaml.safe_load(fd), fname)

    def dumpd(self):
        deps = [x.dumpd(self.cwd) for x in self.deps]
        outs = [x.dumpd(self.cwd) for x in self.outs]

        return {
            Stage.PARAM_CMD: self.cmd,
            Stage.PARAM_DEPS: deps,
            Stage.PARAM_OUTS: outs,
        }

    def dump(self, fname=None):
        if not fname:
            fname = self.path

        with open(fname, 'w') as fd:
            yaml.safe_dump(self.dumpd(), fd, default_flow_style=False)

    def save(self):
        for dep in self.deps:
            dep.update()

        for out in self.outs:
            out.update()
            out.save()
            if out.use_cache:
                self.project.scm.ignore(out.path)

    def run(self):
        if not self.is_data_source:
            self.project.logger.info(u'Reproducing \'{}\':\n\t{}'.format(self.relpath, self.cmd))

            p = subprocess.Popen(self.cmd, cwd=self.cwd, shell=True)
            p.communicate()
            if p.returncode != 0:
               raise StageCmdFailedError(self)
 
            self.save()

            self.project.logger.debug(u'\'{}\' was reproduced'.format(self.relpath))
        else:
            self.project.logger.info(u'Verifying data sources in \'{}\''.format(self.relpath))
            self.check_missing_outputs()
            self.save()

    def check_missing_outputs(self):
        missing_outs = [out.rel_path for out in self.outs if not os.path.exists(out.rel_path)]
        if missing_outs:
            raise MissingDataSource(missing_outs)

    def checkout(self):
        for out in self.outs:
            out.checkout()
