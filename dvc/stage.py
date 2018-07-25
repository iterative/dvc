import os
import yaml
import itertools
import subprocess
from schema import Schema, SchemaError, Optional, Or, And
import posixpath

import dvc.dependency as dependency
import dvc.output as output
from dvc.exceptions import DvcException
from dvc.logger import Logger
from dvc.utils import dict_md5


class StageCmdFailedError(DvcException):
    def __init__(self, stage):
        msg = u'Stage \'{}\' cmd {} failed'.format(stage.relpath, stage.cmd)
        super(StageCmdFailedError, self).__init__(msg)


class StageFileFormatError(DvcException):
    def __init__(self):
        super(StageFileFormatError, self).__init__('Stage file format error')


class MissingDataSource(DvcException):
    def __init__(self, missing_files):
        assert len(missing_files) > 0

        source = 'source'
        if len(missing_files) > 1:
            source += 's'

        msg = u'missing data {}: {}'.format(source, ', '.join(missing_files))
        super(MissingDataSource, self).__init__(msg)


class Stage(object):
    STAGE_FILE = 'Dvcfile'
    STAGE_FILE_SUFFIX = '.dvc'

    PARAM_MD5 = 'md5'
    PARAM_CMD = 'cmd'
    PARAM_DEPS = 'deps'
    PARAM_OUTS = 'outs'
    PARAM_LOCKED = 'locked'

    SCHEMA = {
        Optional(PARAM_MD5): Or(str, None),
        Optional(PARAM_CMD): Or(str, None),
        Optional(PARAM_DEPS): Or(And(list, Schema([dependency.SCHEMA])), None),
        Optional(PARAM_OUTS): Or(And(list, Schema([output.SCHEMA])), None),
        Optional(PARAM_LOCKED): bool,
    }

    def __init__(self,
                 project,
                 path=None,
                 cmd=None,
                 cwd=os.curdir,
                 deps=[],
                 outs=[],
                 md5=None,
                 locked=False):
        self.project = project
        self.path = path
        self.cmd = cmd
        self.cwd = cwd
        self.outs = outs
        self.deps = deps
        self.md5 = md5
        self.locked = locked

    @property
    def relpath(self):
        return os.path.relpath(self.path)

    @property
    def is_data_source(self):
        return self.cmd is None

    @staticmethod
    def is_stage_file(path):
        if not os.path.isfile(path):
            return False

        if not path.endswith(Stage.STAGE_FILE_SUFFIX) \
           and os.path.basename(path) != Stage.STAGE_FILE:
            return False

        return True

    def changed_md5(self):
        md5 = self.dumpd().get(self.PARAM_MD5, None)

        # backward compatibility
        if self.md5 is None:
            return False

        if self.md5 and md5 and self.md5 == md5:
            return False

        msg = "Dvc file '{}' md5 changed(expected '{}', actual '{}')"
        self.project.logger.debug(msg.format(self.relpath, self.md5, md5))
        return True

    @property
    def is_callback(self):
        return not self.is_data_source and len(self.deps) == 0

    @property
    def is_import(self):
        return not self.cmd and \
               len(self.deps) == 1 and \
               len(self.outs) == 1

    def changed(self):
        ret = False

        if self.is_callback:
            ret = True

        if self.locked:
            entries = self.outs
        else:
            entries = itertools.chain(self.outs, self.deps)

        for entry in entries:
            if entry.changed():
                ret = True

        if self.changed_md5():
            ret = True

        if ret:
            msg = u'Dvc file \'{}\' changed'.format(self.relpath)
        else:
            msg = u'Dvc file \'{}\' didn\'t change'.format(self.relpath)

        self.project.logger.debug(msg)

        return ret

    def remove_outs(self, ignore_remove=False):
        for out in self.outs:
            out.remove(ignore_remove=ignore_remove)

    def remove(self):
        self.remove_outs(ignore_remove=True)
        os.unlink(self.path)

    def reproduce(self, force=False):
        if not self.changed() and not force:
            return None

        if (self.cmd or self.is_import) and not self.locked:
            # Removing outputs only if we actually have command to reproduce
            self.remove_outs(ignore_remove=False)

        self.project.logger.info(u'Reproducing \'{}\''.format(self.relpath))

        self.run()

        msg = u'\'{}\' was reproduced'.format(self.relpath)
        self.project.logger.debug(msg)

        return self

    @staticmethod
    def validate(d):
        try:
            Schema(Stage.SCHEMA).validate(d)
        except SchemaError as exc:
            Logger.debug(str(exc))
            raise StageFileFormatError()

    @staticmethod
    def loadd(project, d, path):
        Stage.validate(d)

        path = os.path.abspath(path)
        cwd = os.path.dirname(path)
        cmd = d.get(Stage.PARAM_CMD, None)
        md5 = d.get(Stage.PARAM_MD5, None)
        locked = d.get(Stage.PARAM_LOCKED, False)

        stage = Stage(project=project,
                      path=path,
                      cmd=cmd,
                      cwd=cwd,
                      md5=md5,
                      locked=locked)

        stage.deps = dependency.loadd_from(stage, d.get(Stage.PARAM_DEPS, []))
        stage.outs = output.loadd_from(stage, d.get(Stage.PARAM_OUTS, []))

        return stage

    @classmethod
    def _stage_fname_cwd(cls, fname, cwd, outs, add):
        if fname and cwd:
            return (fname, cwd)

        if not outs:
            return (cls.STAGE_FILE, cwd if cwd else os.getcwd())

        out = outs[0]
        if out.path_info['scheme'] == 'local':
            path = os.path
        else:
            path = posixpath

        if not fname:
            fname = path.basename(out.path) + cls.STAGE_FILE_SUFFIX

        cwd = path.dirname(out.path) if not cwd or add else cwd

        return (fname, cwd)

    @staticmethod
    def loads(project=None,
              cmd=None,
              deps=[],
              outs=[],
              outs_no_cache=[],
              metrics_no_cache=[],
              fname=None,
              cwd=os.curdir,
              locked=False,
              add=False):
        stage = Stage(project=project,
                      cwd=cwd,
                      cmd=cmd,
                      locked=locked)

        stage.outs = output.loads_from(stage, outs, use_cache=True)
        stage.outs += output.loads_from(stage, outs_no_cache, use_cache=False)
        stage.outs += output.loads_from(stage, metrics_no_cache,
                                        use_cache=False, metric=True)
        stage.deps = dependency.loads_from(stage, deps)

        fname, cwd = Stage._stage_fname_cwd(fname, cwd, stage.outs, add=add)

        cwd = os.path.abspath(cwd)
        path = os.path.join(cwd, fname)

        stage.cwd = cwd
        stage.path = path

        return stage

    @staticmethod
    def load(project, fname):
        with open(fname, 'r') as fd:
            return Stage.loadd(project, yaml.safe_load(fd), fname)

    def dumpd(self):
        deps = [x.dumpd() for x in self.deps]
        outs = [x.dumpd() for x in self.outs]

        ret = {}
        if self.cmd is not None:
            ret[Stage.PARAM_CMD] = self.cmd

        if len(deps):
            ret[Stage.PARAM_DEPS] = deps

        if len(outs):
            ret[Stage.PARAM_OUTS] = outs

        if self.locked:
            ret[Stage.PARAM_LOCKED] = self.locked

        ret[Stage.PARAM_MD5] = dict_md5(ret)

        return ret

    def dump(self, fname=None):
        if not fname:
            fname = self.path

        with open(fname, 'w') as fd:
            yaml.safe_dump(self.dumpd(), fd, default_flow_style=False)

    def save(self):
        for dep in self.deps:
            dep.save()

        for out in self.outs:
            out.save()

    def run(self):
        if self.locked:
            msg = u'Verifying outputs in locked stage \'{}\''
            self.project.logger.info(msg.format(self.relpath))
            self.check_missing_outputs()
        elif self.is_import:
            msg = u'Importing \'{}\' -> \'{}\''
            self.project.logger.info(msg.format(self.deps[0].path,
                                                self.outs[0].path))

            self.deps[0].download(self.outs[0].path_info)
        elif self.is_data_source:
            msg = u'Verifying data sources in \'{}\''.format(self.relpath)
            self.project.logger.info(msg)
            self.check_missing_outputs()
        else:
            msg = u'Running command:\n\t{}'.format(self.cmd)
            self.project.logger.info(msg)

            p = subprocess.Popen(self.cmd,
                                 cwd=self.cwd,
                                 shell=True,
                                 env=os.environ,
                                 executable=os.getenv('SHELL'))
            p.communicate()
            if p.returncode != 0:
                raise StageCmdFailedError(self)

        self.save()

    def check_missing_outputs(self):
        outs = [out for out in self.outs if not out.exists]
        paths = [out.path if out.path_info['scheme'] != 'local' else
                 out.rel_path for out in outs]
        if paths:
            raise MissingDataSource(paths)

    def checkout(self):
        for out in self.outs:
            out.checkout()

    def _status(self, entries, name):
        ret = {}

        for entry in entries:
            ret.update(entry.status())

        if ret:
            return {name: ret}

        return {}

    def status(self):
        ret = {}

        if not self.locked:
            ret.update(self._status(self.deps, 'deps'))

        ret.update(self._status(self.outs, 'outs'))

        if ret or self.changed_md5() or self.is_callback:
            return {self.relpath: ret}

        return {}
