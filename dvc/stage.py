import os
import yaml
import itertools
import posixpath
import subprocess
from schema import Schema, SchemaError, Optional, Or, And

import dvc.dependency as dependency
import dvc.output as output
from dvc.exceptions import DvcException
from dvc.logger import Logger
from dvc.utils import dict_md5, fix_env


class StageCmdFailedError(DvcException):
    def __init__(self, stage):
        msg = u'Stage \'{}\' cmd {} failed'.format(stage.relpath, stage.cmd)
        super(StageCmdFailedError, self).__init__(msg)


class StageFileFormatError(DvcException):
    def __init__(self):
        super(StageFileFormatError, self).__init__('Stage file format error')


class StageFileDoesNotExistError(DvcException):
    def __init__(self, fname):
        msg = "'{}' does not exist.".format(fname)
        super(StageFileDoesNotExistError, self).__init__(msg)


class StageFileIsNotDvcFileError(DvcException):
    def __init__(self, fname):
        msg = "'{}' is not a dvc file".format(fname)
        super(StageFileIsNotDvcFileError, self).__init__(msg)


class StageFileBadNameError(DvcException):
    def __init__(self, msg):
        super(StageFileBadNameError, self).__init__(msg)


class MissingDep(DvcException):
    def __init__(self, deps):
        assert len(deps) > 0

        if len(deps) > 1:
            dep = 'dependencies'
        else:
            dep = 'dependency'

        msg = u'missing {}: {}'.format(dep, ', '.join(map(str, deps)))
        super(MissingDep, self).__init__(msg)


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
    def is_stage_filename(path):
        if not path.endswith(Stage.STAGE_FILE_SUFFIX) \
           and os.path.basename(path) != Stage.STAGE_FILE:
            return False

        return True

    @staticmethod
    def is_stage_file(path):
        if not os.path.isfile(path):
            return False

        return Stage.is_stage_filename(path)

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

    def reproduce(self, force=False, dry=False, interactive=False):
        if not self.changed() and not force:
            return None

        if (self.cmd or self.is_import) and not self.locked and not dry:
            # Removing outputs only if we actually have command to reproduce
            self.remove_outs(ignore_remove=False)

        msg = "Going to reproduce '{}'. Are you sure you want to continue?"
        msg = msg.format(self.relpath)
        if interactive \
           and not self.project.prompt.prompt(msg):
            raise DvcException('Reproduction aborted by the user')

        self.project.logger.info(u'Reproducing \'{}\''.format(self.relpath))

        self.run(dry=dry)

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

        if not cwd or (add and out.is_local):
            cwd = path.dirname(out.path)

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
              add=False,
              overwrite=True):
        stage = Stage(project=project,
                      cwd=cwd,
                      cmd=cmd,
                      locked=locked)

        stage.outs = output.loads_from(stage, outs, use_cache=True)
        stage.outs += output.loads_from(stage, outs_no_cache, use_cache=False)
        stage.outs += output.loads_from(stage, metrics_no_cache,
                                        use_cache=False, metric=True)
        stage.deps = dependency.loads_from(stage, deps)

        if fname is not None and os.path.basename(fname) != fname:
            msg = "Stage file name '{}' should not contain subdirectories. " \
                  "Use '-c|--cwd' to change location of the stage file."
            raise StageFileBadNameError(msg.format(fname))

        fname, cwd = Stage._stage_fname_cwd(fname, cwd, stage.outs, add=add)

        cwd = os.path.abspath(cwd)
        path = os.path.join(cwd, fname)

        if os.path.exists(path):
            relpath = os.path.relpath(path)
            msg = "'{}' already exists. " \
                  "Do you wish to run the command and overwrite it?"
            if not overwrite \
               and not project.prompt.prompt(msg.format(relpath), False):
                raise DvcException("'{}' already exists".format(relpath))

        stage.cwd = cwd
        stage.path = path

        return stage

    @staticmethod
    def _check_dvc_filename(fname):
        if not Stage.is_stage_filename(fname):
            msg = "Bad stage filename '{}'. Stage files should be named " \
                  "'Dvcfile' or have a '.dvc' suffix(e.g. '{}.dvc')."
            raise StageFileBadNameError(msg.format(os.path.relpath(fname),
                                                   os.path.basename(fname)))

    @staticmethod
    def _check_dvc_file(fname):
        sname = fname + Stage.STAGE_FILE_SUFFIX
        if Stage.is_stage_file(sname):
            Logger.info("Do you mean '{}'?".format(sname))

    @staticmethod
    def load(project, fname):
        if not os.path.exists(fname):
            Stage._check_dvc_file(fname)
            raise StageFileDoesNotExistError(fname)

        Stage._check_dvc_filename(fname)

        if not Stage.is_stage_file(fname):
            Stage._check_dvc_file(fname)
            raise StageFileIsNotDvcFileError(fname)

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

        self._check_dvc_filename(fname)

        msg = "Saving information to '{}'.".format(os.path.relpath(fname))
        Logger.info(msg)

        with open(fname, 'w') as fd:
            yaml.safe_dump(self.dumpd(), fd, default_flow_style=False)

        self.project._files_to_git_add.append(os.path.relpath(fname))

    def save(self):
        for dep in self.deps:
            dep.save()

        for out in self.outs:
            out.save()

    def _check_missing_deps(self):
        missing = []
        for dep in self.deps:
            if not dep.exists:
                missing.append(dep)

        if len(missing) > 0:
            raise MissingDep(missing)

    def run(self, dry=False):
        if self.locked:
            msg = u'Verifying outputs in locked stage \'{}\''
            self.project.logger.info(msg.format(self.relpath))
            if not dry:
                self.check_missing_outputs()
        elif self.is_import:
            msg = u'Importing \'{}\' -> \'{}\''
            self.project.logger.info(msg.format(self.deps[0].path,
                                                self.outs[0].path))

            if not dry:
                self.deps[0].download(self.outs[0].path_info)
        elif self.is_data_source:
            msg = u'Verifying data sources in \'{}\''.format(self.relpath)
            self.project.logger.info(msg)
            if not dry:
                self.check_missing_outputs()
        else:
            msg = u'Running command:\n\t{}'.format(self.cmd)
            self.project.logger.info(msg)

            if not dry:
                self._check_missing_deps()
                p = subprocess.Popen(self.cmd,
                                     cwd=self.cwd,
                                     shell=True,
                                     env=fix_env(os.environ),
                                     executable=os.getenv('SHELL'))
                p.communicate()
                if p.returncode != 0:
                    raise StageCmdFailedError(self)

        if not dry:
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
