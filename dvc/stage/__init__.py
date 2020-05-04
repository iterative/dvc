import pathlib
import logging
import os
import signal
import subprocess
import threading

from itertools import chain, product

from funcy import project

import dvc.dependency as dependency
import dvc.output as output
import dvc.prompt as prompt
from dvc.exceptions import CheckoutError, DvcException
from .decorators import rwlocked, unlocked_repo
from .exceptions import (
    StageCmdFailedError,
    StagePathOutsideError,
    StagePathNotFoundError,
    StagePathNotDirectoryError,
    StageCommitError,
    StageUpdateError,
    MissingDataSource,
)
from . import params
from dvc.utils import dict_md5
from dvc.utils import fix_env
from dvc.utils import relpath
from dvc.utils.fs import path_isin
from .params import OutputParams

logger = logging.getLogger(__name__)


def loads_from(cls, repo, path, wdir, data):
    kw = {
        "repo": repo,
        "path": path,
        "wdir": wdir,
        **project(
            data,
            [
                Stage.PARAM_CMD,
                Stage.PARAM_LOCKED,
                Stage.PARAM_ALWAYS_CHANGED,
                Stage.PARAM_MD5,
                "name",
            ],
        ),
    }
    return cls(**kw)


def create_stage(cls, repo, path, **kwargs):
    from dvc.dvcfile import check_dvc_filename

    wdir = os.path.abspath(kwargs.get("wdir", None) or os.curdir)
    path = os.path.abspath(path)
    check_dvc_filename(path)
    cls._check_stage_path(repo, wdir, is_wdir=kwargs.get("wdir"))
    cls._check_stage_path(repo, os.path.dirname(path))

    stage = loads_from(cls, repo, path, wdir, kwargs)
    stage._fill_stage_outputs(**kwargs)
    stage._fill_stage_dependencies(**kwargs)
    stage._check_circular_dependency()
    stage._check_duplicated_arguments()

    if stage and stage.dvcfile.exists():
        has_persist_outs = any(out.persist for out in stage.outs)
        ignore_build_cache = (
            kwargs.get("ignore_build_cache", False) or has_persist_outs
        )
        if has_persist_outs:
            logger.warning("Build cache is ignored when persisting outputs.")

        if not ignore_build_cache and stage.can_be_skipped:
            logger.info("Stage is cached, skipping.")
            return None

    return stage


class Stage(params.StageParams):
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
        always_changed=False,
        stage_text=None,
        dvcfile=None,
    ):
        if deps is None:
            deps = []
        if outs is None:
            outs = []

        self.repo = repo
        self._path = path
        self.cmd = cmd
        self.wdir = wdir
        self.outs = outs
        self.deps = deps
        self.md5 = md5
        self.locked = locked
        self.always_changed = always_changed
        self._stage_text = stage_text
        self._dvcfile = dvcfile

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, path):
        self._path = path

    @property
    def dvcfile(self):
        if self.path and self._dvcfile and self.path == self._dvcfile.path:
            return self._dvcfile

        if not self.path:
            raise DvcException(
                "Stage does not have any path set "
                "and is detached from dvcfile."
            )

        from dvc.dvcfile import Dvcfile

        self._dvcfile = Dvcfile(self.repo, self.path)
        return self._dvcfile

    @dvcfile.setter
    def dvcfile(self, dvcfile):
        self._dvcfile = dvcfile

    def __repr__(self):
        return "Stage: '{path}'".format(
            path=self.path_in_repo if self.path else "No path"
        )

    def __str__(self):
        return "stage: '{path}'".format(
            path=self.relpath if self.path else "No path"
        )

    @property
    def addressing(self):
        """
        Useful for alternative presentations where we don't need
        `Stage:` prefix.
        """
        return self.relpath

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

        return isinstance(self.deps[0], dependency.RepoDependency)

    def _changed_deps(self):
        if self.locked:
            return False

        if self.is_callback:
            logger.warning(
                '{stage} is a "callback" stage '
                "(has a command and no dependencies) and thus always "
                "considered as changed.".format(stage=self)
            )
            return True

        if self.always_changed:
            return True

        for dep in self.deps:
            status = dep.status()
            if status:
                logger.debug(
                    "Dependency '{dep}' of {stage} changed because it is "
                    "'{status}'.".format(
                        dep=dep, stage=self, status=status[str(dep)]
                    )
                )
                return True

        return False

    def _changed_outs(self):
        for out in self.outs:
            status = out.status()
            if status:
                logger.debug(
                    "Output '{out}' of {stage} changed because it is "
                    "'{status}'".format(
                        out=out, stage=self, status=status[str(out)]
                    )
                )
                return True

        return False

    def stage_changed(self, warn=False):
        changed = self.md5 != self._compute_md5()
        if changed and warn:
            logger.warning("DVC-file '{}' changed.".format(self.relpath))
        return changed

    @rwlocked(read=["deps", "outs"])
    def changed(self):
        if self._changed():
            logger.warning("{} changed.".format(self))
            return True

        logger.debug("{} didn't change.".format(self))
        return False

    def _changed(self):
        # Short-circuit order: stage md5 is fast, deps are expected to change
        return (
            self.stage_changed(warn=True)
            or self._changed_deps()
            or self._changed_outs()
        )

    @rwlocked(write=["outs"])
    def remove_outs(self, ignore_remove=False, force=False):
        """Used mainly for `dvc remove --outs` and :func:`Stage.reproduce`."""
        for out in self.outs:
            if out.persist and not force:
                out.unprotect()
            else:
                logger.debug(
                    "Removing output '{out}' of {stage}.".format(
                        out=out, stage=self
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
        self.dvcfile.remove()

    @rwlocked(read=["deps"], write=["outs"])
    def reproduce(self, interactive=False, **kwargs):

        if not kwargs.get("force", False) and not self.changed():
            return None

        msg = (
            "Going to reproduce {stage}. "
            "Are you sure you want to continue?".format(stage=self)
        )

        if interactive and not prompt.confirm(msg):
            raise DvcException("reproduction aborted by the user")

        self.run(**kwargs)

        logger.debug("{stage} was reproduced".format(stage=self))

        return self

    def update(self, rev=None):
        if not self.is_repo_import and not self.is_import:
            raise StageUpdateError(self.relpath)

        self.deps[0].update(rev=rev)
        locked = self.locked
        self.locked = False
        try:
            self.reproduce()
        finally:
            self.locked = locked

    @staticmethod
    def _check_stage_path(repo, path, is_wdir=False):
        assert repo is not None

        error_msg = "{wdir_or_path} '{path}' {{}}".format(
            wdir_or_path="stage working dir" if is_wdir else "file path",
            path=path,
        )

        real_path = os.path.realpath(path)
        if not os.path.exists(real_path):
            raise StagePathNotFoundError(error_msg.format("does not exist"))

        if not os.path.isdir(real_path):
            raise StagePathNotDirectoryError(
                error_msg.format("is not directory")
            )

        proj_dir = os.path.realpath(repo.root_dir)
        if real_path != proj_dir and not path_isin(real_path, proj_dir):
            raise StagePathOutsideError(
                error_msg.format("is outside of DVC repo")
            )

    @property
    def can_be_skipped(self):
        return (
            self.is_cached and not self.is_callback and not self.always_changed
        )

    def reload(self):
        return self.dvcfile.stage

    @property
    def is_cached(self):
        """
        Checks if this stage has been already ran and stored
        """
        from dvc.remote.local import LocalRemote
        from dvc.remote.s3 import S3Remote

        old = self.reload()
        if old._changed_outs():
            return False

        # NOTE: need to save checksums for deps in order to compare them
        # with what is written in the old stage.
        self._save_deps()

        old_d = old.dumpd()
        new_d = self.dumpd()

        # NOTE: need to remove checksums from old dict in order to compare
        # it to the new one, since the new one doesn't have checksums yet.
        old_d.pop(self.PARAM_MD5, None)
        new_d.pop(self.PARAM_MD5, None)
        outs = old_d.get(self.PARAM_OUTS, [])
        for out in outs:
            out.pop(LocalRemote.PARAM_CHECKSUM, None)
            out.pop(S3Remote.PARAM_CHECKSUM, None)

        # outs and deps are lists of dicts. To check equality, we need to make
        # them independent of the order, so, we convert them to dicts.
        combination = product(
            [old_d, new_d], [self.PARAM_DEPS, self.PARAM_OUTS]
        )
        for coll, key in combination:
            if coll.get(key):
                coll[key] = {item["path"]: item for item in coll[key]}

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

    def _fill_stage_outputs(self, **kwargs):
        assert not self.outs

        self.outs = []
        for key in (p.value for p in OutputParams):
            self.outs += output.loads_from(
                self,
                kwargs.get(key, []),
                use_cache="no_cache" not in key,
                persist="persist" in key,
                metric="metrics" in key,
            )

    def _fill_stage_dependencies(self, **kwargs):
        assert not self.deps
        self.deps = []
        self.deps += dependency.loads_from(
            self, kwargs.get("deps", []), erepo=kwargs.get("erepo", None)
        )
        self.deps += dependency.loads_params(self, kwargs.get("params", []))

    def _fix_outs_deps_path(self, wdir):
        for out in chain(self.outs, self.deps):
            if out.is_in_repo:
                out.def_path = relpath(out.path_info, wdir)

    def resolve_wdir(self):
        rel_wdir = relpath(self.wdir, os.path.dirname(self.path))
        return (
            pathlib.PurePath(rel_wdir).as_posix() if rel_wdir != "." else None
        )

    def dumpd(self):
        return {
            key: value
            for key, value in {
                Stage.PARAM_MD5: self.md5,
                Stage.PARAM_CMD: self.cmd,
                Stage.PARAM_WDIR: self.resolve_wdir(),
                Stage.PARAM_LOCKED: self.locked,
                Stage.PARAM_DEPS: [d.dumpd() for d in self.deps],
                Stage.PARAM_OUTS: [o.dumpd() for o in self.outs],
                Stage.PARAM_ALWAYS_CHANGED: self.always_changed,
            }.items()
            if value
        }

    def _compute_md5(self):
        from dvc.output.base import BaseOutput

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
        # pipeline. Not excluding `LocalOutput.PARAM_CACHE`, because if
        # it has changed, we might not have that output in our cache.
        m = dict_md5(
            d,
            exclude=[
                self.PARAM_LOCKED,
                BaseOutput.PARAM_METRIC,
                BaseOutput.PARAM_PERSIST,
            ],
        )
        logger.debug("Computed {} md5: '{}'".format(self, m))
        return m

    def _save_deps(self):
        for dep in self.deps:
            dep.save()

    def save(self):
        self._save_deps()

        for out in self.outs:
            out.save()

        self.md5 = self._compute_md5()

        self.repo.stage_cache.save(self)

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

        if changed_deps or changed_outs or self.stage_changed():
            msg = (
                "dependencies {}".format(changed_deps) if changed_deps else ""
            )
            msg += " and " if (changed_deps and changed_outs) else ""
            msg += "outputs {}".format(changed_outs) if changed_outs else ""
            msg += "md5" if not (changed_deps or changed_outs) else ""
            msg += " of {} changed. ".format(self)
            msg += "Are you sure you want to commit it?"
            if not force and not prompt.confirm(msg):
                raise StageCommitError(
                    "unable to commit changed {}. Use `-f|--force` to "
                    "force.".format(self)
                )
            self.save()

    @rwlocked(write=["outs"])
    def commit(self):
        for out in self.outs:
            out.commit()

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

        retcode = None if not p else p.returncode
        if retcode != 0:
            raise StageCmdFailedError(self, retcode)

    @rwlocked(read=["deps"], write=["outs"])
    def run(
        self, dry=False, no_commit=False, force=False, ignore_build_cache=False
    ):
        if (self.cmd or self.is_import) and not self.locked and not dry:
            self.remove_outs(ignore_remove=False, force=False)

        if self.locked:
            logger.info(
                "Verifying outputs in locked {stage}".format(stage=self)
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
                if (
                    not force
                    and not self.stage_changed(warn=True)
                    and self._already_cached()
                ):
                    self.outs[0].checkout()
                else:
                    self.deps[0].download(self.outs[0])
        elif self.is_data_source:
            msg = "Verifying data sources in {}".format(self)
            logger.info(msg)
            if not dry:
                self.check_missing_outputs()

        else:
            if not dry:
                stage_cache = self.repo.stage_cache
                stage_cached = (
                    not force
                    and not self.is_callback
                    and not self.always_changed
                    and self._already_cached()
                )
                use_build_cache = False
                if not stage_cached:
                    self._save_deps()
                    use_build_cache = (
                        not force
                        and not ignore_build_cache
                        and stage_cache.is_cached(self)
                    )

                if use_build_cache:
                    # restore stage from build cache
                    self.repo.stage_cache.restore(self)
                    stage_cached = self._outs_cached()

                if stage_cached:
                    logger.info("Stage is cached, skipping.")
                    self.checkout()
                else:
                    logger.info("Running command:\n\t{}".format(self.cmd))
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
        checkouts = {"failed": [], "added": [], "modified": []}
        for out in self._filter_outs(filter_info):
            try:
                result = out.checkout(
                    force=force,
                    progress_callback=progress_callback,
                    relink=relink,
                    filter_info=filter_info,
                )
                added, modified = result or (None, None)
                if modified:
                    checkouts["modified"].append(out.path_info)
                elif added:
                    checkouts["added"].append(out.path_info)
            except CheckoutError as exc:
                checkouts["failed"].extend(exc.target_infos)

        return checkouts

    @staticmethod
    def _status(entries):
        ret = {}

        for entry in entries:
            ret.update(entry.status())

        return ret

    def stage_status(self):
        return ["changed checksum"] if self.stage_changed() else []

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

        ret.extend(self.stage_status())
        if self.is_callback or self.always_changed:
            ret.append("always changed")

        if ret:
            return {self.addressing: ret}

        return {}

    def _already_cached(self):
        return self._deps_cached() and self._outs_cached()

    def _deps_cached(self):
        return all(not dep.changed() for dep in self.deps)

    def _outs_cached(self):
        return all(
            not out.changed_cache() if out.use_cache else not out.changed()
            for out in self.outs
        )

    def get_all_files_number(self, filter_info=None):
        return sum(
            out.get_files_number(filter_info)
            for out in self._filter_outs(filter_info)
        )

    def get_used_cache(self, *args, **kwargs):
        from dvc.cache import NamedCache

        cache = NamedCache()
        for out in self._filter_outs(kwargs.get("filter_info")):
            cache.update(out.get_used_cache(*args, **kwargs))

        return cache


class PipelineStage(Stage):
    def __init__(self, *args, name=None, meta=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.cmd_changed = False
        # This is how the Stage will discover any discrepancies
        self.meta = meta or {}

    def __eq__(self, other):
        return super().__eq__(other) and self.name == other.name

    def __hash__(self):
        return hash((self.path_in_repo, self.name))

    def __repr__(self):
        return "Stage: '{path}:{name}'".format(
            path=self.relpath if self.path else "No path", name=self.name
        )

    def __str__(self):
        return "stage: '{path}:{name}'".format(
            path=self.relpath if self.path else "No path", name=self.name
        )

    @property
    def addressing(self):
        return super().addressing + ":" + self.name

    def reload(self):
        return self.dvcfile.stages[self.name]

    @property
    def is_cached(self):
        return self.name in self.dvcfile.stages and super().is_cached

    def stage_status(self):
        return ["changed command"] if self.cmd_changed else []

    def stage_changed(self, warn=False):
        if self.cmd_changed and warn:
            logger.warning("'cmd' of {} has changed.".format(self))
        return self.cmd_changed
