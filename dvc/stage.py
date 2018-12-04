import os
import yaml
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
    def __init__(self, fname, e):
        msg = "Stage file '{}' format error: {}".format(fname, str(e))
        super(StageFileFormatError, self).__init__(msg)


class StageFileDoesNotExistError(DvcException):
    def __init__(self, fname):
        msg = "'{}' does not exist.".format(fname)
        super(StageFileDoesNotExistError, self).__init__(msg)


class StageFileAlreadyExistsError(DvcException):
    def __init__(self, relpath):
        msg = "Stage '{}' already exists".format(relpath)
        super(StageFileAlreadyExistsError, self).__init__(msg)


class StageFileIsNotDvcFileError(DvcException):
    def __init__(self, fname):
        msg = "'{}' is not a dvc file".format(fname)
        super(StageFileIsNotDvcFileError, self).__init__(msg)


class StageFileBadNameError(DvcException):
    def __init__(self, msg):
        super(StageFileBadNameError, self).__init__(msg)


class StageBadCwdError(DvcException):
    def __init__(self, cwd):
        msg = "Stage cwd '{}' is outside of the current dvc project"
        super(StageBadCwdError, self).__init__(msg.format(cwd))


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
        md5 = self._get_md5()
        assert md5 is not None

        if self.md5 == md5:
            return False

        return True

    @property
    def is_callback(self):
        return not self.is_data_source and len(self.deps) == 0

    @property
    def is_import(self):
        return not self.cmd and \
               len(self.deps) == 1 and \
               len(self.outs) == 1

    def _changed_deps(self, log):
        if self.locked:
            return False

        if self.is_callback:
            msg = "Dvc file '{}' is a 'callback' stage (has a command and " \
                  "no dependencies) and thus always considered as changed."
            self.project.logger.warn(msg.format(self.relpath))
            return True

        for dep in self.deps:
            if not dep.changed():
                continue
            log("Dependency '{}' of '{}' changed.".format(dep, self.relpath))
            return True

        return False

    def _changed_outs(self, log):
        for out in self.outs:
            if not out.changed():
                continue
            log("Output '{}' of '{}' changed.".format(out, self.relpath))
            return True

        return False

    def _changed_md5(self, log):
        if self.changed_md5():
            log("Dvc file '{}' changed.".format(self.relpath))
            return True
        return False

    def changed(self, print_info=False):
        if print_info:
            log = self.project.logger.info
        else:
            log = self.project.logger.debug

        ret = any([self._changed_deps(log),
                   self._changed_outs(log),
                   self._changed_md5(log)])

        if ret:
            msg = "Stage '{}' changed.".format(self.relpath)
            color = 'yellow'
        else:
            msg = "Stage '{}' didn't change.".format(self.relpath)
            color = 'green'

        log(Logger.colorize(msg, color))

        return ret

    def remove_outs(self, ignore_remove=False):
        for out in self.outs:
            out.remove(ignore_remove=ignore_remove)

    def unprotect_outs(self):
        for out in self.outs:
            if out.path_info['scheme'] != 'local' or not out.exists:
                continue
            self.project.unprotect(out.path)

    def remove(self):
        self.remove_outs(ignore_remove=True)
        os.unlink(self.path)

    def reproduce(self, force=False, dry=False, interactive=False):
        if not self.changed(print_info=True) and not force:
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
    def validate(d, fname=None):
        try:
            Schema(Stage.SCHEMA).validate(d)
        except SchemaError as exc:
            raise StageFileFormatError(fname, exc)

    @staticmethod
    def loadd(project, d, path):
        Stage.validate(d, fname=os.path.relpath(path))
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
    def _check_inside_project(project, cwd):
        assert project is not None
        proj_dir = os.path.realpath(project.root_dir)
        if not os.path.realpath(cwd).startswith(proj_dir):
            raise StageBadCwdError(cwd)

    def is_cached(self):
        """
        Checks if this stage has been already ran and saved to the same
        dvc file.
        """
        from dvc.remote.local import RemoteLOCAL
        from dvc.remote.s3 import RemoteS3

        old = Stage.load(self.project, self.path)
        if old._changed_outs(log=self.project.logger.debug):
            return False

        # NOTE: need to save checksums for deps in order to compare them
        # with what is written in the old stage.
        for dep in self.deps:
            dep.save()

        old_d = old.dumpd()
        new_d = self.dumpd()

        # NOTE: need to remove checksums from old dict in order to compare
        # it to the new one, since the new one doesn't have checksums yet.
        old_d.pop(self.PARAM_MD5, None)
        new_d.pop(self.PARAM_MD5, None)
        outs = old_d.get(self.PARAM_OUTS, [])
        for out in outs:
            out.pop(RemoteLOCAL.PARAM_MD5, None)
            out.pop(RemoteS3.PARAM_ETAG, None)

        return old_d == new_d

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
              overwrite=True,
              ignore_build_cache=False,
              remove_outs=False):

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

        Stage._check_inside_project(project, cwd)

        cwd = os.path.abspath(cwd)
        path = os.path.join(cwd, fname)

        stage.cwd = cwd
        stage.path = path

        # NOTE: remove outs before we check build cache
        if remove_outs:
            stage.remove_outs(ignore_remove=False)
            project.logger.warn("Build cache is ignored when using "
                                "--remove-outs.")
            ignore_build_cache = True
        else:
            stage.unprotect_outs()

        if os.path.exists(path):
            if not ignore_build_cache and stage.is_cached():
                Logger.info('Stage is cached, skipping.')
                return None

            msg = "'{}' already exists. Do you wish to run the command and " \
                  "overwrite it?".format(stage.relpath)
            if not overwrite and not project.prompt.prompt(msg, False):
                raise StageFileAlreadyExistsError(stage.relpath)

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
            return Stage.loadd(project, yaml.safe_load(fd) or dict(), fname)

    def dumpd(self):
        ret = {}

        if self.cmd is not None:
            ret[Stage.PARAM_CMD] = self.cmd

        if len(self.deps):
            ret[Stage.PARAM_DEPS] = [d.dumpd() for d in self.deps]

        if len(self.outs):
            ret[Stage.PARAM_OUTS] = [o.dumpd() for o in self.outs]

        ret[Stage.PARAM_MD5] = self.md5

        if self.locked:
            ret[Stage.PARAM_LOCKED] = self.locked

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

    def _get_md5(self):
        from dvc.output.local import OutputLOCAL

        # NOTE: excluding parameters that don't affect the state of the
        # pipeline. Not excluding OutputLOCAL.PARAM_CACHE, because if
        # it has changed, we might not have that output in our cache.
        exclude = [self.PARAM_LOCKED,
                   OutputLOCAL.PARAM_METRIC]

        d = self.dumpd()

        # NOTE: removing md5 manually in order to not affect md5s in deps/outs
        if self.PARAM_MD5 in d.keys():
            del d[self.PARAM_MD5]

        return dict_md5(d, exclude)

    def save(self):
        for dep in self.deps:
            dep.save()

        for out in self.outs:
            out.save()

        self.md5 = self._get_md5()

    def _check_missing_deps(self):
        missing = []
        for dep in self.deps:
            if not dep.exists:
                missing.append(dep)

        if len(missing) > 0:
            raise MissingDep(missing)

    def _check_if_fish(self, executable):  # pragma: no cover
        if executable is None \
           or os.path.basename(os.path.realpath(executable)) != 'fish':
            return

        msg = "DVC detected that you are using fish as your default " \
              "shell. Be aware that it might cause problems by overwriting " \
              "your current environment variables with values defined " \
              "in '.fishrc', which might affect your command. See " \
              "https://github.com/iterative/dvc/issues/1307. "
        self.project.logger.warn(msg)

    def _run(self):
        self._check_missing_deps()
        executable = os.getenv('SHELL') if os.name != 'nt' else None
        self._check_if_fish(executable)

        p = subprocess.Popen(self.cmd,
                             cwd=self.cwd,
                             shell=True,
                             env=fix_env(os.environ),
                             executable=executable)
        p.communicate()

        if p.returncode != 0:
            raise StageCmdFailedError(self)

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
                self._run()

        if not dry:
            self.save()

    def check_missing_outputs(self):
        outs = [out for out in self.outs if not out.exists]
        paths = [out.path if out.path_info['scheme'] != 'local' else
                 out.rel_path for out in outs]
        if paths:
            raise MissingDataSource(paths)

    def checkout(self, force=False):
        for out in self.outs:
            out.checkout(force=force)

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
