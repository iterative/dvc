import pathlib
import logging
import os
import re
import signal
import subprocess
import threading

from functools import wraps
from itertools import chain
from funcy import decorator

from voluptuous import Any, Schema, MultipleInvalid

import dvc.dependency as dependency
import dvc.output as output
import dvc.prompt as prompt
from dvc.exceptions import DvcException
from dvc.utils import dict_md5
from dvc.utils import fix_env
from dvc.utils import relpath
from dvc.utils.fs import path_isin
from dvc.utils.collections import apply_diff
from dvc.utils.fs import contains_symlink_up_to
from dvc.utils.stage import dump_stage_file
from dvc.utils.stage import parse_stage
from dvc.utils.stage import parse_stage_for_update


logger = logging.getLogger(__name__)


class StageCmdFailedError(DvcException):
    def __init__(self, stage):
        msg = "stage '{}' cmd '{}' failed".format(stage.relpath, stage.cmd)
        super().__init__(msg)


class StageFileFormatError(DvcException):
    def __init__(self, fname, e):
        msg = "DVC-file '{}' format error: {}".format(fname, str(e))
        super().__init__(msg)


class StageFileDoesNotExistError(DvcException):
    def __init__(self, fname):
        msg = "'{}' does not exist.".format(fname)

        sname = fname + Stage.STAGE_FILE_SUFFIX
        if Stage.is_stage_file(sname):
            msg += " Do you mean '{}'?".format(sname)

        super().__init__(msg)


class StageFileAlreadyExistsError(DvcException):
    def __init__(self, relpath):
        msg = "stage '{}' already exists".format(relpath)
        super().__init__(msg)


class StageFileIsNotDvcFileError(DvcException):
    def __init__(self, fname):
        msg = "'{}' is not a DVC-file".format(fname)

        sname = fname + Stage.STAGE_FILE_SUFFIX
        if Stage.is_stage_file(sname):
            msg += " Do you mean '{}'?".format(sname)

        super().__init__(msg)


class StageFileBadNameError(DvcException):
    pass


class StagePathOutsideError(DvcException):
    def __init__(self, path):
        msg = "stage working or file path '{}' is outside of DVC repo"
        super().__init__(msg.format(path))


class StagePathNotFoundError(DvcException):
    def __init__(self, path):
        msg = "stage working or file path '{}' does not exist"
        super().__init__(msg.format(path))


class StagePathNotDirectoryError(DvcException):
    def __init__(self, path):
        msg = "stage working or file path '{}' is not directory"
        super().__init__(msg.format(path))


class StageCommitError(DvcException):
    pass


class StageUpdateError(DvcException):
    def __init__(self, path):
        super().__init__(
            "update is not supported for '{}' that is not an "
            "import.".format(path)
        )


class MissingDep(DvcException):
    def __init__(self, deps):
        assert len(deps) > 0

        if len(deps) > 1:
            dep = "dependencies"
        else:
            dep = "dependency"

        msg = "missing '{}': {}".format(dep, ", ".join(map(str, deps)))
        super().__init__(msg)


class MissingDataSource(DvcException):
    def __init__(self, missing_files):
        assert len(missing_files) > 0

        source = "source"
        if len(missing_files) > 1:
            source += "s"

        msg = "missing data '{}': {}".format(source, ", ".join(missing_files))
        super().__init__(msg)


@decorator
def rwlocked(call, read=None, write=None):
    import sys
    from dvc.rwlock import rwlock
    from dvc.dependency.repo import DependencyREPO

    if read is None:
        read = []

    if write is None:
        write = []

    stage = call._args[0]

    assert stage.repo.lock.is_locked

    def _chain(names):
        return [
            item.path_info
            for attr in names
            for item in getattr(stage, attr)
            # There is no need to lock DependencyREPO deps, as there is no
            # corresponding OutputREPO, so we can't even write it.
            if not isinstance(item, DependencyREPO)
        ]

    cmd = " ".join(sys.argv)

    with rwlock(stage.repo.tmp_dir, cmd, _chain(read), _chain(write)):
        return call()


def unlocked_repo(f):
    @wraps(f)
    def wrapper(stage, *args, **kwargs):
        stage.repo.state.dump()
        stage.repo.lock.unlock()
        stage.repo._reset()
        try:
            ret = f(stage, *args, **kwargs)
        finally:
            stage.repo.lock.lock()
            stage.repo.state.load()
        return ret

    return wrapper


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
    PARAM_ALWAYS_CHANGED = "always_changed"

    SCHEMA = {
        PARAM_MD5: Any(str, None),
        PARAM_CMD: Any(str, None),
        PARAM_WDIR: Any(str, None),
        PARAM_DEPS: Any([dependency.SCHEMA], None),
        PARAM_OUTS: Any([output.SCHEMA], None),
        PARAM_LOCKED: bool,
        PARAM_META: object,
        PARAM_ALWAYS_CHANGED: bool,
    }
    COMPILED_SCHEMA = Schema(SCHEMA)

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
        always_changed=False,
        stage_text=None,
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
        self.always_changed = always_changed
        self._stage_text = stage_text

    def __repr__(self):
        return "Stage: '{path}'".format(
            path=self.relpath if self.path else "No path"
        )

    def __hash__(self):
        return hash(self.path_in_repo)

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__
            and self.repo is other.repo
            and self.path_in_repo == other.path_in_repo
        )

    @property
    def path_in_repo(self):
        return relpath(self.path, self.repo.root_dir)

    @property
    def relpath(self):
        return relpath(self.path)

    @property
    def is_data_source(self):
        """Whether the DVC-file was created with `dvc add` or `dvc import`"""
        return self.cmd is None

    @staticmethod
    def is_valid_filename(path):
        return (
            path.endswith(Stage.STAGE_FILE_SUFFIX)
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
        """Whether the DVC-file was created with `dvc import`."""
        return not self.cmd and len(self.deps) == 1 and len(self.outs) == 1

    @property
    def is_repo_import(self):
        if not self.is_import:
            return False

        return isinstance(self.deps[0], dependency.DependencyREPO)

    def _changed_deps(self):
        if self.locked:
            return False

        if self.is_callback:
            logger.warning(
                "DVC-file '{fname}' is a \"callback\" stage "
                "(has a command and no dependencies) and thus always "
                "considered as changed.".format(fname=self.relpath)
            )
            return True

        if self.always_changed:
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
            logger.warning("DVC-file '{}' changed.".format(self.relpath))
            return True
        return False

    @rwlocked(read=["deps", "outs"])
    def changed(self):
        # Short-circuit order: stage md5 is fast, deps are expected to change
        ret = (
            self._changed_md5() or self._changed_deps() or self._changed_outs()
        )

        if ret:
            logger.warning("Stage '{}' changed.".format(self.relpath))
        else:
            logger.debug("Stage '{}' didn't change.".format(self.relpath))

        return ret

    @rwlocked(write=["outs"])
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

    @rwlocked(write=["outs"])
    def remove(self, force=False, remove_outs=True):
        if remove_outs:
            self.remove_outs(ignore_remove=True, force=force)
        else:
            self.unprotect_outs()
        os.unlink(self.path)

    @rwlocked(read=["deps"], write=["outs"])
    def reproduce(self, interactive=False, **kwargs):

        if not kwargs.get("force", False) and not self.changed():
            return None

        msg = (
            "Going to reproduce '{stage}'. "
            "Are you sure you want to continue?".format(stage=self.relpath)
        )

        if interactive and not prompt.confirm(msg):
            raise DvcException("reproduction aborted by the user")

        self.run(**kwargs)

        logger.debug("'{stage}' was reproduced".format(stage=self.relpath))

        return self

    def update(self):
        if not self.is_repo_import and not self.is_import:
            raise StageUpdateError(self.relpath)

        self.deps[0].update()
        locked = self.locked
        self.locked = False
        try:
            self.reproduce()
        finally:
            self.locked = locked

    @staticmethod
    def validate(d, fname=None):
        try:
            Stage.COMPILED_SCHEMA(d)
        except MultipleInvalid as exc:
            raise StageFileFormatError(fname, exc)

    @classmethod
    def _stage_fname(cls, outs, add):
        if not outs:
            return cls.STAGE_FILE

        out = outs[0]
        fname = out.path_info.name + cls.STAGE_FILE_SUFFIX

        if (
            add
            and out.is_in_repo
            and not contains_symlink_up_to(out.fspath, out.repo.root_dir)
        ):
            fname = out.path_info.with_name(fname).fspath

        return fname

    @staticmethod
    def _check_stage_path(repo, path):
        assert repo is not None

        real_path = os.path.realpath(path)
        if not os.path.exists(real_path):
            raise StagePathNotFoundError(path)

        if not os.path.isdir(real_path):
            raise StagePathNotDirectoryError(path)

        proj_dir = os.path.realpath(repo.root_dir)
        if real_path != proj_dir and not path_isin(real_path, proj_dir):
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
    def create(repo, **kwargs):

        wdir = kwargs.get("wdir", None)
        cwd = kwargs.get("cwd", None)
        fname = kwargs.get("fname", None)
        add = kwargs.get("add", False)

        # Backward compatibility for `cwd` option
        if wdir is None and cwd is not None:
            if fname is not None and os.path.basename(fname) != fname:
                raise StageFileBadNameError(
                    "DVC-file name '{fname}' may not contain subdirectories"
                    " if `-c|--cwd` (deprecated) is specified. Use `-w|--wdir`"
                    " along with `-f` to specify DVC-file path with working"
                    " directory.".format(fname=fname)
                )
            wdir = cwd
        elif wdir is None:
            wdir = os.curdir

        stage = Stage(
            repo=repo,
            wdir=wdir,
            cmd=kwargs.get("cmd", None),
            locked=kwargs.get("locked", False),
            always_changed=kwargs.get("always_changed", False),
        )

        Stage._fill_stage_outputs(stage, **kwargs)
        stage.deps = dependency.loads_from(
            stage, kwargs.get("deps", []), erepo=kwargs.get("erepo", None)
        )

        stage._check_circular_dependency()
        stage._check_duplicated_arguments()

        if not fname:
            fname = Stage._stage_fname(stage.outs, add)
        stage._check_dvc_filename(fname)

        # Autodetecting wdir for add, we need to create outs first to do that,
        # so we start with wdir = . and remap out paths later.
        if add and kwargs.get("wdir") is None and cwd is None:
            wdir = os.path.dirname(fname)

            for out in chain(stage.outs, stage.deps):
                if out.is_in_repo:
                    out.def_path = relpath(out.path_info, wdir)

        wdir = os.path.abspath(wdir)

        if cwd is not None:
            path = os.path.join(wdir, fname)
        else:
            path = os.path.abspath(fname)

        Stage._check_stage_path(repo, wdir)
        Stage._check_stage_path(repo, os.path.dirname(path))

        stage.wdir = wdir
        stage.path = path

        ignore_build_cache = kwargs.get("ignore_build_cache", False)

        # NOTE: remove outs before we check build cache
        if kwargs.get("remove_outs", False):
            logger.warning(
                "--remove-outs is deprecated."
                " It is now the default behavior,"
                " so there's no need to use this option anymore."
            )
            stage.remove_outs(ignore_remove=False)
            logger.warning("Build cache is ignored when using --remove-outs.")
            ignore_build_cache = True

        if os.path.exists(path) and any(out.persist for out in stage.outs):
            logger.warning("Build cache is ignored when persisting outputs.")
            ignore_build_cache = True

        if os.path.exists(path):
            if (
                not ignore_build_cache
                and stage.is_cached
                and not stage.is_callback
                and not stage.always_changed
            ):
                logger.info("Stage is cached, skipping.")
                return None

            msg = (
                "'{}' already exists. Do you wish to run the command and "
                "overwrite it?".format(stage.relpath)
            )

            if not kwargs.get("overwrite", True) and not prompt.confirm(msg):
                raise StageFileAlreadyExistsError(stage.relpath)

            os.unlink(path)

        return stage

    @staticmethod
    def _fill_stage_outputs(stage, **kwargs):
        stage.outs = output.loads_from(
            stage, kwargs.get("outs", []), use_cache=True
        )
        stage.outs += output.loads_from(
            stage, kwargs.get("metrics", []), use_cache=True, metric=True
        )
        stage.outs += output.loads_from(
            stage, kwargs.get("outs_persist", []), use_cache=True, persist=True
        )
        stage.outs += output.loads_from(
            stage, kwargs.get("outs_no_cache", []), use_cache=False
        )
        stage.outs += output.loads_from(
            stage,
            kwargs.get("metrics_no_cache", []),
            use_cache=False,
            metric=True,
        )
        stage.outs += output.loads_from(
            stage,
            kwargs.get("outs_persist_no_cache", []),
            use_cache=False,
            persist=True,
        )

    @staticmethod
    def _check_dvc_filename(fname):
        if not Stage.is_valid_filename(fname):
            raise StageFileBadNameError(
                "bad DVC-file name '{}'. DVC-files should be named "
                "'Dvcfile' or have a '.dvc' suffix (e.g. '{}.dvc').".format(
                    relpath(fname), os.path.basename(fname)
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
        # 2. filename is not a DVC-file
        # 3. path doesn't represent a regular file
        Stage._check_file_exists(repo, fname)
        Stage._check_dvc_filename(fname)
        Stage._check_isfile(repo, fname)

        with repo.tree.open(fname) as fd:
            stage_text = fd.read()
        d = parse_stage(stage_text, fname)

        Stage.validate(d, fname=relpath(fname))
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
            always_changed=d.get(Stage.PARAM_ALWAYS_CHANGED, False),
            # We store stage text to apply updates to the same structure
            stage_text=stage_text,
        )

        stage.deps = dependency.loadd_from(
            stage, d.get(Stage.PARAM_DEPS) or []
        )
        stage.outs = output.loadd_from(stage, d.get(Stage.PARAM_OUTS) or [])

        return stage

    def dumpd(self):
        rel_wdir = relpath(self.wdir, os.path.dirname(self.path))

        wdir = pathlib.PurePath(rel_wdir).as_posix()
        wdir = wdir if wdir != "." else None

        return {
            key: value
            for key, value in {
                Stage.PARAM_MD5: self.md5,
                Stage.PARAM_CMD: self.cmd,
                Stage.PARAM_WDIR: wdir,
                Stage.PARAM_LOCKED: self.locked,
                Stage.PARAM_DEPS: [d.dumpd() for d in self.deps],
                Stage.PARAM_OUTS: [o.dumpd() for o in self.outs],
                Stage.PARAM_ALWAYS_CHANGED: self.always_changed,
            }.items()
            if value
        }

    def dump(self):
        fname = self.path

        self._check_dvc_filename(fname)

        logger.debug(
            "Saving information to '{file}'.".format(file=relpath(fname))
        )
        state = self.dumpd()

        # When we load a stage we parse yaml with a fast parser, which strips
        # off all the comments and formatting. To retain those on update we do
        # a trick here:
        # - reparse the same yaml text with a slow but smart ruamel yaml parser
        # - apply changes to a returned structure
        # - serialize it
        if self._stage_text is not None:
            saved_state = parse_stage_for_update(self._stage_text, fname)
            # Stage doesn't work with meta in any way, so .dumpd() doesn't
            # have it. We simply copy it over.
            if "meta" in saved_state:
                state["meta"] = saved_state["meta"]
            apply_diff(state, saved_state)
            state = saved_state

        dump_stage_file(fname, state)

        self.repo.scm.track_file(relpath(fname))

    def _compute_md5(self):
        from dvc.output.base import OutputBase

        d = self.dumpd()

        # Remove md5 and meta, these should not affect stage md5
        d.pop(self.PARAM_MD5, None)
        d.pop(self.PARAM_META, None)

        # Ignore the wdir default value. In this case DVC-file w/o
        # wdir has the same md5 as a file with the default value specified.
        # It's important for backward compatibility with pipelines that
        # didn't have WDIR in their DVC-files.
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
        return [
            str(entry)
            for entry in entries
            if entry.checksum and entry.changed_checksum()
        ]

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
            msg += " of '{}' changed. ".format(self.relpath)
            msg += "Are you sure you want to commit it?"
            if not force and not prompt.confirm(msg):
                raise StageCommitError(
                    "unable to commit changed '{}'. Use `-f|--force` to "
                    "force.".format(self.relpath)
                )
            self.save()

    @rwlocked(write=["outs"])
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

        circular_dependencies = set(d.path_info for d in self.deps) & set(
            o.path_info for o in self.outs
        )

        if circular_dependencies:
            raise CircularDependencyError(str(circular_dependencies.pop()))

    def _check_duplicated_arguments(self):
        from dvc.exceptions import ArgumentDuplicationError
        from collections import Counter

        path_counts = Counter(edge.path_info for edge in self.deps + self.outs)

        for path, occurrence in path_counts.items():
            if occurrence > 1:
                raise ArgumentDuplicationError(str(path))

    @unlocked_repo
    def _run(self):
        self._check_missing_deps()

        kwargs = {"cwd": self.wdir, "env": fix_env(None), "close_fds": True}

        if os.name == "nt":
            kwargs["shell"] = True
            cmd = self.cmd
        else:
            # NOTE: when you specify `shell=True`, `Popen` [1] will default to
            # `/bin/sh` on *nix and will add ["/bin/sh", "-c"] to your command.
            # But we actually want to run the same shell that we are running
            # from right now, which is usually determined by the `SHELL` env
            # var. So instead, we compose our command on our own, making sure
            # to include special flags to prevent shell from reading any
            # configs and modifying env, which may change the behavior or the
            # command we are running. See [2] for more info.
            #
            # [1] https://github.com/python/cpython/blob/3.7/Lib/subprocess.py
            #                                                            #L1426
            # [2] https://github.com/iterative/dvc/issues/2506
            #                                           #issuecomment-535396799
            kwargs["shell"] = False
            executable = os.getenv("SHELL") or "/bin/sh"

            self._warn_if_fish(executable)

            opts = {"zsh": ["--no-rcs"], "bash": ["--noprofile", "--norc"]}
            name = os.path.basename(executable).lower()
            cmd = [executable] + opts.get(name, []) + ["-c", self.cmd]

        main_thread = isinstance(
            threading.current_thread(), threading._MainThread
        )
        old_handler = None
        p = None

        try:
            p = subprocess.Popen(cmd, **kwargs)
            if main_thread:
                old_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
            p.communicate()
        finally:
            if old_handler:
                signal.signal(signal.SIGINT, old_handler)

        if (p is None) or (p.returncode != 0):
            raise StageCmdFailedError(self)

    @rwlocked(read=["deps"], write=["outs"])
    def run(self, dry=False, no_commit=False, force=False):
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
                    dep=self.deps[0], out=self.outs[0]
                )
            )
            if not dry:
                if not force and self._already_cached():
                    self.outs[0].checkout()
                else:
                    self.deps[0].download(self.outs[0])
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
                    and not self.always_changed
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
        paths = [str(out) for out in self.outs if not out.exists]
        if paths:
            raise MissingDataSource(paths)

    def _filter_outs(self, path_info):
        def _func(o):
            return path_info.isin_or_eq(o.path_info)

        return filter(_func, self.outs) if path_info else self.outs

    @rwlocked(write=["outs"])
    def checkout(
        self,
        force=False,
        progress_callback=None,
        relink=False,
        filter_info=None,
    ):
        failed_checkouts = []
        for out in self._filter_outs(filter_info):
            failed = out.checkout(
                force=force,
                tag=self.tag,
                progress_callback=progress_callback,
                relink=relink,
                filter_info=filter_info,
            )
            if failed:
                failed_checkouts.append(failed)
        return failed_checkouts

    @staticmethod
    def _status(entries):
        ret = {}

        for entry in entries:
            ret.update(entry.status())

        return ret

    @rwlocked(read=["deps", "outs"])
    def status(self, check_updates=False):
        ret = []

        show_import = self.is_repo_import and check_updates

        if not self.locked or show_import:
            deps_status = self._status(self.deps)
            if deps_status:
                ret.append({"changed deps": deps_status})

        outs_status = self._status(self.outs)
        if outs_status:
            ret.append({"changed outs": outs_status})

        if self.changed_md5():
            ret.append("changed checksum")

        if self.is_callback or self.always_changed:
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

    def get_all_files_number(self, filter_info=None):
        return sum(
            out.get_files_number(filter_info)
            for out in self._filter_outs(filter_info)
        )

    def get_used_cache(self, *args, **kwargs):
        from .cache import NamedCache

        cache = NamedCache()
        for out in self._filter_outs(kwargs.get("filter_info")):
            cache.update(out.get_used_cache(*args, **kwargs))

        return cache
