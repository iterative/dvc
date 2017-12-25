import os
import stat
import yaml
import itertools

from dvc.system import System
from dvc.data_cloud import file_md5
from dvc.exceptions import DvcException
from dvc.executor import Executor


class OutputNoCacheError(DvcException):
    pass


class Output(object):
    PARAM_PATH = 'path'
    PARAM_MD5 = 'md5'
    PARAM_CACHE = 'cache'

    def __init__(self, project, path, md5=None, use_cache=False):
        self.project = project
        self.path = path
        self.md5 = md5
        self.use_cache = use_cache

    def _changed_md5(self):
        return self.md5 != file_md5(self.path)[0]

    def changed(self):
        if not self.use_cache:
            changed = self._changed_md5()
        else:
            changed = os.path.exists(self.path) \
                      and os.path.exists(self.cache) \
                      and not os.path.samefile(self.path, self.cache)

        if changed:
            self.project.logger.debug('{} changed'.format(self.path))

        return changed

    @property
    def cache(self):
        return self.project.cache.get(self.md5)

    def link(self, checkout=False):
        if not self.use_cache:
            raise OutputNoCacheError()

        if not os.path.exists(self.path) and not os.path.exists(self.cache):
            raise OutputNoCacheError()

        if os.path.exists(self.path) and os.path.exists(self.cache) and os.path.samefile(self.path, self.cache):
            return

        if os.path.exists(self.cache):
            if os.path.exists(self.path):
                # This means that we already have cache for this data.
                # We remove data and link it to existing cache to save
                # some space.
                os.unlink(self.path)
            src = self.cache
            link = self.path
        elif not checkout:
            src = self.path
            link = self.cache
        else:
            raise OutputNoCacheError()

        System.hardlink(src, link)

    def checkout(self):
        if not self.use_cache:
            return
        self.link(checkout=True)

    def mtime(self):
        return os.path.getmtime(self.path)

    def update(self, md5=None):
        self.md5 = md5
        if not self.md5:
            self.md5 = file_md5(self.path)[0]
        self.project.state.update(self.path, self.md5, self.mtime())

    def save(self):
        if not self.use_cache:
            return

        self.project.scm.ignore(self.path)
        self.link()
        os.chmod(self.path, stat.S_IREAD)

    def dumpd(self, cwd):
        return {
            Output.PARAM_PATH: os.path.relpath(self.path, cwd),
            Output.PARAM_MD5: self.md5,
            Output.PARAM_CACHE: self.use_cache
        }

    @classmethod
    def loadd(cls, project, d, cwd='.'):
        path = os.path.join(cwd, d[Output.PARAM_PATH])
        md5 = d[Output.PARAM_MD5]
        use_cache = d[Output.PARAM_CACHE]
        return cls(project, path, md5, use_cache=use_cache)

    @classmethod
    def loadd_from(cls, project, d_list, cwd='.'):
        return [cls.loadd(project, x, cwd=cwd) for x in d_list]

    @classmethod
    def loads(cls, project, s, use_cache=False, cwd='.'):
        return cls(project, os.path.join(cwd, s), None, use_cache=use_cache)

    @classmethod
    def loads_from(cls, project, s_list, use_cache=False, cwd='.'):
        return [cls.loads(project, x, use_cache, cwd=cwd) for x in s_list]

    def stage(self):
        for stage in self.project.stages():
            for out in stage.outs:
                if self.path == out.path:
                    return stage
        return None


class Dependency(Output):
    def update(self):
        md5 = None
        state = self.project.state.get(self.path)
        if state and state.mtime == self.mtime():
            md5 = state.md5
            msg = '{} using md5 from state file for dependency'
            self.project.logger.debug(msg.format(self.path))

        super(Dependency, self).update(md5=md5)


class Stage(object):
    STAGE_FILE = 'Dvcfile'
    STAGE_FILE_SUFFIX = '.dvc'

    PARAM_CMD = 'cmd'
    PARAM_DEPS = 'deps'
    PARAM_OUTS = 'outs'
    PARAM_LOCKED = 'locked'

    def __init__(self, project, path=None, cmd=None, cwd=None, deps=[], outs=[], locked=False):
        self.project = project
        self.path = path
        self.cmd = cmd
        self.cwd = cwd
        self.outs = outs
        self.deps = deps
        self.locked = locked

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
            if os.path.exists(out.path):
                self.project.logger.debug("Removing '{}'".format(out.path))
                os.unlink(out.path)

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
        self.project.logger.debug("{} reproduced".format(self.path))

    @staticmethod
    def loadd(project, d, path):
        path = os.path.abspath(path)
        cwd = os.path.dirname(path)
        cmd = d[Stage.PARAM_CMD]
        deps = Dependency.loadd_from(project, d[Stage.PARAM_DEPS], cwd=cwd)
        outs = Output.loadd_from(project, d[Stage.PARAM_OUTS], cwd=cwd)
        locked = d[Stage.PARAM_LOCKED]

        return Stage(project=project,
                     path=path,
                     cmd=cmd,
                     cwd=cwd,
                     deps=deps,
                     outs=outs,
                     locked=locked)

    @staticmethod
    def load(project, fname):
        with open(fname, 'r') as fd:
            return Stage.loadd(project, yaml.load(fd), fname)

    def dumpd(self):
        deps = [x.dumpd(self.cwd) for x in self.deps]
        outs = [x.dumpd(self.cwd) for x in self.outs]

        return {
            Stage.PARAM_CMD: self.cmd,
            Stage.PARAM_DEPS: deps,
            Stage.PARAM_OUTS: outs,
            Stage.PARAM_LOCKED: self.locked
        }

    def dump(self, fname=None):
        if not fname:
            fname = self.path

        with open(fname, 'w') as fd:
            yaml.dump(self.dumpd(), fd, default_flow_style=False)

    def save(self):
        for dep in self.deps:
            dep.update()
            dep.save()

        for out in self.outs:
            out.update()
            out.save()

    def run(self):
        if self.cmd:
            Executor.exec_cmd_only_success(self.cmd, cwd=str(self.cwd), shell=True)
        self.save()

    def checkout(self):
        for entry in itertools.chain(self.outs, self.deps):
            entry.checkout()
