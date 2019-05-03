from __future__ import unicode_literals

from dvc.utils.compat import str

import copy
import re
import os
import subprocess
import logging

from dvc.utils.fs import contains_symlink_up_to
from schema import Schema, SchemaError, Optional, Or, And

import dvc.prompt as prompt
import dvc.dependency as dependency
import dvc.output as output
from dvc.exceptions import DvcException
from dvc.utils import dict_md5, fix_env
from dvc.utils.collections import apply_diff
from dvc.utils.stage import load_stage_fd, dump_stage_file


logger = logging.getLogger(__name__)


class StageCmdFailedError(DvcException):
    def __init__(self, stage):
        msg = "stage '{}' cmd {} failed".format(stage.relpath, stage.cmd)
        super(StageCmdFailedError, self).__init__(msg)


class StageFileFormatError(DvcException):
    def __init__(self, fname, e):
        msg = "stage file '{}' format error: {}".format(fname, str(e))
        super(StageFileFormatError, self).__init__(msg)


class StageFileDoesNotExistError(DvcException):
    def __init__(self, fname):
        msg = "'{}' does not exist.".format(fname)

        sname = fname + Stage.STAGE_FILE_SUFFIX
        if Stage.is_stage_file(sname):
            msg += " Do you mean '{}'?".format(sname)

        super(StageFileDoesNotExistError, self).__init__(msg)


class StageFileAlreadyExistsError(DvcException):
    def __init__(self, relpath):
        msg = "stage '{}' already exists".format(relpath)
        super(StageFileAlreadyExistsError, self).__init__(msg)


class StageFileIsNotDvcFileError(DvcException):
    def __init__(self, fname):
        msg = "'{}' is not a dvc file".format(fname)

        sname = fname + Stage.STAGE_FILE_SUFFIX
        if Stage.is_stage_file(sname):
            msg += " Do you mean '{}'?".format(sname)

        super(StageFileIsNotDvcFileError, self).__init__(msg)


class StageFileBadNameError(DvcException):
    def __init__(self, msg):
        super(StageFileBadNameError, self).__init__(msg)


class StagePathOutsideError(DvcException):
    def __init__(self, path):
        msg = "stage working or file path '{}' is outside of dvc repo"
        super(StagePathOutsideError, self).__init__(msg.format(path))


class StagePathNotFoundError(DvcException):
    def __init__(self, path):
        msg = "stage working or file path '{}' does not exist"
        super(StagePathNotFoundError, self).__init__(msg.format(path))


class StagePathNotDirectoryError(DvcException):
    def __init__(self, path):
        msg = "stage working or file path '{}' is not directory"
        super(StagePathNotDirectoryError, self).__init__(msg.format(path))


class StageCommitError(DvcException):
    pass


class MissingDep(DvcException):
    def __init__(self, deps):
        assert len(deps) > 0

        if len(deps) > 1:
            dep = "dependencies"
        else:
            dep = "dependency"

        msg = "missing {}: {}".format(dep, ", ".join(map(str, deps)))
        super(MissingDep, self).__init__(msg)


class MissingDataSource(DvcException):
    def __init__(self, missing_files):
        assert len(missing_files) > 0

        source = "source"
        if len(missing_files) > 1:
            source += "s"

        msg = "missing data {}: {}".format(source, ", ".join(missing_files))
        super(MissingDataSource, self).__init__(msg)


class Stage(object):
    STAGE_FILE = "Dvcfile"
    STAGE_FILE_SUFFIX = ".dvc"

    PARAM_MD5 = "md5"
    PARAM_CMD = "cmd"
    PARAM_WDIR = "wdir"
    PARAM_DEPS = "deps"
    PARAM_OUTS = "outs"
    PARAM_LOCKED = "locked"
    PARAM_META = "meta"

    SCHEMA = {
        Optional(PARAM_MD5): Or(str, None),
        Optional(PARAM_CMD): Or(str, None),
        Optional(PARAM_WDIR): Or(str, None),
        Optional(PARAM_DEPS): Or(And(list, Schema([dependency.SCHEMA])), None),
        Optional(PARAM_OUTS): Or(And(list, Schema([output.SCHEMA])), None),
        Optional(PARAM_LOCKED): bool,
        Optional(PARAM_META): object,
    }

    TAG_REGEX = r"^(?P<path>.*)@(?P<tag>[^\\/@:]*)$"

    def __init__(
        self,
        repo,
        path=None,
        cmd=None,
        wdir=os.curdir,
        deps=None,
        outs=None,
        md5=None,
        locked=False,
        tag=None,
        state=None,
    ):
        if deps is None:
            deps = []
        if outs is None:
            outs = []

        self.repo = repo
        self.path = path
        self.cmd = cmd
        self.wdir = wdir
        self.outs = outs
        self.deps = deps
        self.md5 = md5
        self.locked = locked
        self.tag = tag
        self._state = state or {}

    def __repr__(self):
        return "Stage: '{path}'".format(
            path=self.relpath if self.path else "No path"
        )

    @property
    def relpath(self):
        return os.path.relpath(self.path)

    @property
    def is_data_source(self):
        """Whether the stage file was created with `dvc add` or `dvc import`"""
        return self.cmd is None

    @staticmethod
    def is_valid_filename(path):
        return (
            # path.endswith doesn't work for encoded unicode filenames on
            # Python 2 and since Stage.STAGE_FILE_SUFFIX is ascii then it is
            # not needed to decode the path from py2's str
            path[-len(Stage.STAGE_FILE_SUFFIX) :] == Stage.STAGE_FILE_SUFFIX
            or os.path.basename(path) == Stage.STAGE_FILE
        )

    @staticmethod
    def is_stage_file(path):
        return os.path.isfile(path) and Stage.is_valid_filename(path)

    def changed_md5(self):
        return self.md5 != self._compute_md5()

    @property
    def is_callback(self):
        """
        A callback stage is always considered as changed,
        so it runs on every `dvc repro` call.
        """
        return not self.is_data_source and len(self.deps) == 0

    @property
    def is_import(self):
        """Whether the stage file was created with `dvc import`."""
        return not self.cmd and len(self.deps) == 1 and len(self.outs) == 1

    def _changed_deps(self):
        if self.locked:
            return False

        if self.is_callback:
            logger.warning(
                "Dvc file '{fname}' is a 'callback' stage "
                "(has a command and no dependencies) and thus always "
                "considered as changed.".format(fname=self.relpath)
            )
            return True

        for dep in self.deps:
            status = dep.status()
            if status:
                logger.warning(
                    "Dependency '{dep}' of '{stage}' changed because it is "
                    "'{status}'.".format(
                        dep=dep, stage=self.relpath, status=status[str(dep)]
                    )
                )
                return True

        return False

    def _changed_outs(self):
        for out in self.outs:
            status = out.status()
            if status:
                logger.warning(
                    "Output '{out}' of '{stage}' changed because it is "
                    "'{status}'".format(
                        out=out, stage=self.relpath, status=status[str(out)]
                    )
                )
                return True

        return False

    def _changed_md5(self):
        if self.changed_md5():
            logger.warning("Dvc file '{}' changed.".format(self.relpath))
            return True
        return False

    def changed(self):
        ret = any(
            [self._changed_deps(), self._changed_outs(), self._changed_md5()]
        )

        if ret:
            logger.warning("Stage '{}' changed.".format(self.relpath))
        else:
            logger.info("Stage '{}' didn't change.".format(self.relpath))

        return ret

    def remove_outs(self, ignore_remove=False, force=False):
        """Used mainly for `dvc remove --outs` and :func:`Stage.reproduce`."""
        for out in self.outs:
            if out.persist and not force:
                out.unprotect()
            else:
                logger.debug(
                    "Removing output '{out}' of '{stage}'.".format(
                        out=out, stage=self.relpath
                    )
                )
                out.remove(ignore_remove=ignore_remove)

    def unprotect_outs(self):
        for out in self.outs:
            out.unprotect()

    def remove(self, force=False):
        self.remove_outs(ignore_remove=True, force=force)
        os.unlink(self.path)

    def reproduce(
        self, force=False, dry=False, interactive=False, no_commit=False
    ):
        if not self.changed() and not force:
            return None

        msg = (
            "Going to reproduce '{stage}'. "
            "Are you sure you want to continue?".format(stage=self.relpath)
        )

        if interactive and not prompt.confirm(msg):
            raise DvcException("reproduction aborted by the user")

        logger.info("Reproducing '{stage}'".format(stage=self.relpath))

        self.run(dry=dry, no_commit=no_commit, force=force)

        logger.debug("'{stage}' was reproduced".format(stage=self.relpath))

        return self

    @staticmethod
    def validate(d, fname=None):
        from dvc.utils import convert_to_unicode

        try:
            Schema(Stage.SCHEMA).validate(convert_to_unicode(d))
        except SchemaError as exc:
            raise StageFileFormatError(fname, exc)

    @classmethod
    def _stage_fname(cls, fname, outs, add):
        if fname:
            return fname

        if not outs:
            return cls.STAGE_FILE

        out = outs[0]
        path_handler = out.remote.ospath

        fname = path_handler.basename(out.path) + cls.STAGE_FILE_SUFFIX

        fname = Stage._expand_to_path_on_add_local(
            add, fname, out, path_handler
        )

        return fname

    @staticmethod
    def _expand_to_path_on_add_local(add, fname, out, path_handler):
        if (
            add
            and out.is_in_repo
            and not contains_symlink_up_to(out.path, out.repo.root_dir)
        ):
            fname = path_handler.join(path_handler.dirname(out.path), fname)
        return fname

    @staticmethod
    def _check_stage_path(repo, path):
        assert repo is not None

        real_path = os.path.realpath(path)
        if not os.path.exists(real_path):
            raise StagePathNotFoundError(path)

        if not os.path.isdir(real_path):
            raise StagePathNotDirectoryError(path)

        proj_dir = os.path.realpath(repo.root_dir) + os.path.sep
        if not (real_path + os.path.sep).startswith(proj_dir):
            raise StagePathOutsideError(path)

    @property
    def is_cached(self):
        """
        Checks if this stage has been already ran and stored
        """
        from dvc.remote.local import RemoteLOCAL
        from dvc.remote.s3 import RemoteS3

        old = Stage.load(self.repo, self.path)
        if old._changed_outs():
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
            out.pop(RemoteLOCAL.PARAM_CHECKSUM, None)
            out.pop(RemoteS3.PARAM_CHECKSUM, None)

        if old_d != new_d:
            return False

        # NOTE: committing to prevent potential data duplication. For example
        #
        #    $ dvc config cache.type hardlink
        #    $ echo foo > foo
        #    $ dvc add foo
        #    $ rm -f foo
        #    $ echo foo > foo
        #    $ dvc add foo # should replace foo with a link to cache
        #
        old.commit()

        return True

    @staticmethod
    def create(
        repo=None,
        cmd=None,
        deps=None,
        outs=None,
        outs_no_cache=None,
        metrics=None,
        metrics_no_cache=None,
        fname=None,
        cwd=None,
        wdir=None,
        locked=False,
        add=False,
        overwrite=True,
        ignore_build_cache=False,
        remove_outs=False,
        validate_state=True,
        outs_persist=None,
        outs_persist_no_cache=None,
    ):
        if outs is None:
            outs = []
        if deps is None:
            deps = []
        if outs_no_cache is None:
            outs_no_cache = []
        if metrics is None:
            metrics = []
        if metrics_no_cache is None:
            metrics_no_cache = []
        if outs_persist is None:
            outs_persist = []
        if outs_persist_no_cache is None:
            outs_persist_no_cache = []

        # Backward compatibility for `cwd` option
        if wdir is None and cwd is not None:
            if fname is not None and os.path.basename(fname) != fname:
                raise StageFileBadNameError(
                    "stage file name '{fname}' may not contain subdirectories"
                    " if '-c|--cwd' (deprecated) is specified. Use '-w|--wdir'"
                    " along with '-f' to specify stage file path and working"
                    " directory.".format(fname=fname)
                )
            wdir = cwd
        else:
            wdir = os.curdir if wdir is None else wdir

        stage = Stage(repo=repo, wdir=wdir, cmd=cmd, locked=locked)

        Stage._fill_stage_outputs(
            stage,
            outs,
            outs_no_cache,
            metrics,
            metrics_no_cache,
            outs_persist,
            outs_persist_no_cache,
        )
        stage.deps = dependency.loads_from(stage, deps)

        stage._check_circular_dependency()
        stage._check_duplicated_arguments()

        fname = Stage._stage_fname(fname, stage.outs, add=add)
        wdir = os.path.abspath(wdir)

        if cwd is not None:
            path = os.path.join(wdir, fname)
        else:
            path = os.path.abspath(fname)

        Stage._check_stage_path(repo, wdir)
        Stage._check_stage_path(repo, os.path.dirname(path))

        stage.wdir = wdir
        stage.path = path

        # NOTE: remove outs before we check build cache
        if remove_outs:
            logger.warning(
                "--remove-outs is deprecated."
                " It is now the default behavior,"
                " so there's no need to use this option anymore."
            )
            stage.remove_outs(ignore_remove=False)
            logger.warning("Build cache is ignored when using --remove-outs.")
            ignore_build_cache = True
        else:
            stage.unprotect_outs()

        if os.path.exists(path) and any(out.persist for out in stage.outs):
            logger.warning("Build cache is ignored when persisting outputs.")
            ignore_build_cache = True

        if validate_state:
            if os.path.exists(path):
                if not ignore_build_cache and stage.is_cached:
                    logger.info("Stage is cached, skipping.")
                    return None

                msg = (
                    "'{}' already exists. Do you wish to run the command and "
                    "overwrite it?".format(stage.relpath)
                )

                if not overwrite and not prompt.confirm(msg):
                    raise StageFileAlreadyExistsError(stage.relpath)

                os.unlink(path)

        return stage

    @staticmethod
    def _fill_stage_outputs(
        stage,
        outs,
        outs_no_cache,
        metrics,
        metrics_no_cache,
        outs_persist,
        outs_persist_no_cache,
    ):
        stage.outs = output.loads_from(stage, outs, use_cache=True)
        stage.outs += output.loads_from(
            stage, metrics, use_cache=True, metric=True
        )
        stage.outs += output.loads_from(
            stage, outs_persist, use_cache=True, persist=True
        )
        stage.outs += output.loads_from(stage, outs_no_cache, use_cache=False)
        stage.outs += output.loads_from(
            stage, metrics_no_cache, use_cache=False, metric=True
        )
        stage.outs += output.loads_from(
            stage, outs_persist_no_cache, use_cache=False, persist=True
        )

    @staticmethod
    def _check_dvc_filename(fname):
        if not Stage.is_valid_filename(fname):
            raise StageFileBadNameError(
                "bad stage filename '{}'. Stage files should be named"
                " 'Dvcfile' or have a '.dvc' suffix (e.g. '{}.dvc').".format(
                    os.path.relpath(fname), os.path.basename(fname)
                )
            )

    @staticmethod
    def _check_file_exists(repo, fname):
        if not repo.tree.exists(fname):
            raise StageFileDoesNotExistError(fname)

    @staticmethod
    def _check_isfile(repo, fname):
        if not repo.tree.isfile(fname):
            raise StageFileIsNotDvcFileError(fname)

    @classmethod
    def _get_path_tag(cls, s):
        regex = re.compile(cls.TAG_REGEX)
        match = regex.match(s)
        if not match:
            return s, None
        return match.group("path"), match.group("tag")

    @staticmethod
    def load(repo, fname):
        fname, tag = Stage._get_path_tag(fname)

        # it raises the proper exceptions by priority:
        # 1. when the file doesn't exists
        # 2. filename is not a dvc filename
        # 3. path doesn't represent a regular file
        Stage._check_file_exists(repo, fname)
        Stage._check_dvc_filename(fname)
        Stage._check_isfile(repo, fname)

        with repo.tree.open(fname) as fd:
            d = load_stage_fd(fd, fname)
        # Making a deepcopy since the original structure
        # looses keys in deps and outs load
        state = copy.deepcopy(d)

        Stage.validate(d, fname=os.path.relpath(fname))
        path = os.path.abspath(fname)

        stage = Stage(
            repo=repo,
            path=path,
            wdir=os.path.abspath(
                os.path.join(
                    os.path.dirname(path), d.get(Stage.PARAM_WDIR, ".")
                )
            ),
            cmd=d.get(Stage.PARAM_CMD),
            md5=d.get(Stage.PARAM_MD5),
            locked=d.get(Stage.PARAM_LOCKED, False),
            tag=tag,
            state=state,
        )

        stage.deps = dependency.loadd_from(stage, d.get(Stage.PARAM_DEPS, []))
        stage.outs = output.loadd_from(stage, d.get(Stage.PARAM_OUTS, []))

        return stage

    def dumpd(self):
        from dvc.remote.base import RemoteBase

        return {
            key: value
            for key, value in {
                Stage.PARAM_MD5: self.md5,
                Stage.PARAM_CMD: self.cmd,
                Stage.PARAM_WDIR: RemoteBase.to_posixpath(
                    os.path.relpath(self.wdir, os.path.dirname(self.path))
                ),
                Stage.PARAM_LOCKED: self.locked,
                Stage.PARAM_DEPS: [d.dumpd() for d in self.deps],
                Stage.PARAM_OUTS: [o.dumpd() for o in self.outs],
                Stage.PARAM_META: self._state.get("meta"),
            }.items()
            if value
        }

    def dump(self):
        fname = self.path

        self._check_dvc_filename(fname)

        logger.info(
            "Saving information to '{file}'.".format(
                file=os.path.relpath(fname)
            )
        )
        d = self.dumpd()
        apply_diff(d, self._state)
        dump_stage_file(fname, self._state)

        self.repo.scm.track_file(os.path.relpath(fname))

    def _compute_md5(self):
        from dvc.output.base import OutputBase

        d = self.dumpd()

        # NOTE: removing md5 manually in order to not affect md5s in deps/outs
        if self.PARAM_MD5 in d.keys():
            del d[self.PARAM_MD5]

        # Ignore the wdir default value. In this case stage file w/o
        # wdir has the same md5 as a file with the default value specified.
        # It's important for backward compatibility with pipelines that
        # didn't have WDIR in their stage files.
        if d.get(self.PARAM_WDIR) == ".":
            del d[self.PARAM_WDIR]

        # NOTE: excluding parameters that don't affect the state of the
        # pipeline. Not excluding `OutputLOCAL.PARAM_CACHE`, because if
        # it has changed, we might not have that output in our cache.
        m = dict_md5(
            d,
            exclude=[
                self.PARAM_LOCKED,
                OutputBase.PARAM_METRIC,
                OutputBase.PARAM_TAGS,
                OutputBase.PARAM_PERSIST,
            ],
        )
        logger.debug("Computed stage '{}' md5: '{}'".format(self.relpath, m))
        return m

    def save(self):
        for dep in self.deps:
            dep.save()

        for out in self.outs:
            out.save()

        self.md5 = self._compute_md5()

    @staticmethod
    def _changed_entries(entries):
        ret = []
        for entry in entries:
            if entry.checksum and entry.changed_checksum():
                ret.append(entry.rel_path)
        return ret

    def check_can_commit(self, force):
        changed_deps = self._changed_entries(self.deps)
        changed_outs = self._changed_entries(self.outs)

        if changed_deps or changed_outs or self.changed_md5():
            msg = (
                "dependencies {}".format(changed_deps) if changed_deps else ""
            )
            msg += " and " if (changed_deps and changed_outs) else ""
            msg += "outputs {}".format(changed_outs) if changed_outs else ""
            msg += "md5" if not (changed_deps or changed_outs) else ""
            msg += " of '{}' changed. Are you sure you commit it?".format(
                self.relpath
            )
            if not force and not prompt.confirm(msg):
                raise StageCommitError(
                    "unable to commit changed '{}'. Use `-f|--force` to "
                    "force.`".format(self.relpath)
                )
            self.save()

    def commit(self):
        for out in self.outs:
            out.commit()

    def _check_missing_deps(self):
        missing = [dep for dep in self.deps if not dep.exists]

        if any(missing):
            raise MissingDep(missing)

    @staticmethod
    def _warn_if_fish(executable):  # pragma: no cover
        if (
            executable is None
            or os.path.basename(os.path.realpath(executable)) != "fish"
        ):
            return

        logger.warning(
            "DVC detected that you are using fish as your default "
            "shell. Be aware that it might cause problems by overwriting "
            "your current environment variables with values defined "
            "in '.fishrc', which might affect your command. See "
            "https://github.com/iterative/dvc/issues/1307. "
        )

    def _check_circular_dependency(self):
        from dvc.exceptions import CircularDependencyError

        circular_dependencies = set(d.path for d in self.deps) & set(
            o.path for o in self.outs
        )

        if circular_dependencies:
            raise CircularDependencyError(circular_dependencies.pop())

    def _check_duplicated_arguments(self):
        from dvc.exceptions import ArgumentDuplicationError
        from collections import Counter

        path_counts = Counter(edge.path for edge in self.deps + self.outs)

        for path, occurrence in path_counts.items():
            if occurrence > 1:
                raise ArgumentDuplicationError(path)

    def _run(self):
        self._check_missing_deps()
        executable = os.getenv("SHELL") if os.name != "nt" else None
        self._warn_if_fish(executable)

        p = subprocess.Popen(
            self.cmd,
            cwd=self.wdir,
            shell=True,
            env=fix_env(os.environ),
            executable=executable,
        )
        p.communicate()

        if p.returncode != 0:
            raise StageCmdFailedError(self)

    def run(self, dry=False, resume=False, no_commit=False, force=False):
        if (self.cmd or self.is_import) and not self.locked and not dry:
            self.remove_outs(ignore_remove=False, force=False)

        if self.locked:
            logger.info(
                "Verifying outputs in locked stage '{stage}'".format(
                    stage=self.relpath
                )
            )
            if not dry:
                self.check_missing_outputs()

        elif self.is_import:
            logger.info(
                "Importing '{dep}' -> '{out}'".format(
                    dep=self.deps[0].path, out=self.outs[0].path
                )
            )
            if not dry:
                if self._already_cached() and not force:
                    self.outs[0].checkout()
                else:
                    self.deps[0].download(
                        self.outs[0].path_info, resume=resume
                    )

        elif self.is_data_source:
            msg = "Verifying data sources in '{}'".format(self.relpath)
            logger.info(msg)
            if not dry:
                self.check_missing_outputs()

        else:
            logger.info("Running command:\n\t{}".format(self.cmd))
            if not dry:
                if (
                    not force
                    and not self.is_callback
                    and self._already_cached()
                ):
                    self.checkout()
                else:
                    self._run()

        if not dry:
            self.save()
            if not no_commit:
                self.commit()

    def check_missing_outputs(self):
        paths = [
            out.path if out.scheme != "local" else out.rel_path
            for out in self.outs
            if not out.exists
        ]

        if paths:
            raise MissingDataSource(paths)

    def checkout(self, force=False, progress_callback=None):
        for out in self.outs:
            out.checkout(
                force=force, tag=self.tag, progress_callback=progress_callback
            )

    @staticmethod
    def _status(entries):
        ret = {}

        for entry in entries:
            ret.update(entry.status())

        return ret

    def status(self):
        ret = []

        if not self.locked:
            deps_status = self._status(self.deps)
            if deps_status:
                ret.append({"changed deps": deps_status})

        outs_status = self._status(self.outs)
        if outs_status:
            ret.append({"changed outs": outs_status})

        if self.changed_md5():
            ret.append("changed checksum")

        if self.is_callback:
            ret.append("always changed")

        if ret:
            return {self.relpath: ret}

        return {}

    def _already_cached(self):
        return (
            not self.changed_md5()
            and all(not dep.changed() for dep in self.deps)
            and all(
                not out.changed_cache() if out.use_cache else not out.changed()
                for out in self.outs
            )
        )

    def get_all_files_number(self):
        return sum(out.get_files_number() for out in self.outs)
