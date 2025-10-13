import os
import string
from collections import defaultdict
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, TypeVar, Union

from funcy import project

from dvc import prompt
from dvc.exceptions import CacheLinkError, CheckoutError, DvcException, MergeError
from dvc.log import logger
from dvc.utils import relpath
from dvc.utils.objects import cached_property

from . import params
from .decorators import rwlocked
from .exceptions import StageUpdateError
from .imports import sync_import, update_import
from .run import run_stage
from .utils import (
    check_circular_dependency,
    check_duplicated_arguments,
    check_missing_outputs,
    check_no_externals,
    check_stage_path,
    compute_md5,
    fill_stage_dependencies,
    fill_stage_outputs,
    get_dump,
)

if TYPE_CHECKING:
    from dvc.dependency import Dependency, ParamsDependency
    from dvc.dvcfile import ProjectFile, SingleStageFile
    from dvc.output import Output
    from dvc.repo import Repo
    from dvc.types import StrPath
    from dvc_data.hashfile.db import HashFileDB
    from dvc_data.hashfile.hash_info import HashInfo
    from dvc_objects.db import ObjectDB

logger = logger.getChild(__name__)
# Disallow all punctuation characters except hyphen and underscore
INVALID_STAGENAME_CHARS = set(string.punctuation) - {"_", "-"}
Env = dict[str, str]
ChangedEntries = tuple[list[str], list[str], Optional[str]]

_T = TypeVar("_T")


def loads_from(
    cls: type[_T], repo: "Repo", path: str, wdir: str, data: dict[str, Any]
) -> _T:
    kw = {
        "repo": repo,
        "path": path,
        "wdir": wdir,
        **project(
            data,
            [
                Stage.PARAM_CMD,
                Stage.PARAM_LOCKED,
                Stage.PARAM_FROZEN,
                Stage.PARAM_ALWAYS_CHANGED,
                Stage.PARAM_MD5,
                Stage.PARAM_DESC,
                Stage.PARAM_META,
                "name",
            ],
        ),
    }
    return cls(**kw)


@dataclass
class RawData:
    parametrized: bool = False
    generated_from: Optional[str] = None


def create_stage(cls: type[_T], repo, path, **kwargs) -> _T:
    from dvc.dvcfile import check_dvcfile_path

    wdir = os.path.abspath(kwargs.get("wdir") or os.curdir)
    path = os.path.abspath(path)

    check_dvcfile_path(repo, path)
    check_stage_path(repo, wdir, is_wdir=kwargs.get("wdir"))
    check_stage_path(repo, os.path.dirname(path))

    stage = loads_from(cls, repo, path, wdir, kwargs)
    fill_stage_outputs(stage, **kwargs)
    check_no_externals(stage)
    fill_stage_dependencies(
        stage, **project(kwargs, ["deps", "erepo", "params", "fs_config", "db"])
    )
    check_circular_dependency(stage)
    check_duplicated_arguments(stage)

    return stage


def restore_fields(stage: "Stage") -> None:
    from .exceptions import StageNotFound

    if not stage.dvcfile.exists():
        return

    try:
        old = stage.reload()
    except StageNotFound:
        return

    # will be used to restore comments later

    stage._stage_text = old._stage_text
    stage.meta = old.meta
    stage.desc = old.desc

    old_outs = {out.def_path: out for out in old.outs}
    for out in stage.outs:
        old_out = old_outs.get(out.def_path)
        if old_out is not None:
            out.restore_fields(old_out)


class Stage(params.StageParams):
    def __init__(  # noqa: PLR0913
        self,
        repo,
        path=None,
        cmd=None,
        wdir=os.curdir,
        deps=None,
        outs=None,
        md5=None,
        locked=False,  # backward compatibility
        frozen=False,
        always_changed=False,
        stage_text=None,
        dvcfile=None,
        desc: Optional[str] = None,
        meta=None,
    ):
        if deps is None:
            deps = []
        if outs is None:
            outs = []

        self.repo = repo
        self._path = path
        self.cmd = cmd
        self.wdir = wdir
        self.outs: list[Output] = outs
        self.deps: list[Dependency] = deps
        self.md5 = md5
        self.frozen = locked or frozen
        self.always_changed = always_changed
        self._stage_text = stage_text
        self._dvcfile = dvcfile
        self.desc: Optional[str] = desc
        self.meta = meta
        self.raw_data = RawData()

    @property
    def path(self) -> str:
        return self._path

    @path.setter
    def path(self, path: str):
        self._path = path
        self.__dict__.pop("path_in_repo", None)
        self.__dict__.pop("relpath", None)

    @property
    def dvcfile(self) -> Union["ProjectFile", "SingleStageFile"]:
        if self.path and self._dvcfile and self.path == self._dvcfile.path:
            return self._dvcfile

        if not self.path:
            raise DvcException(
                "Stage does not have any path set and is detached from dvcfile."
            )

        from dvc.dvcfile import load_file

        self._dvcfile = load_file(self.repo, self.path)
        return self._dvcfile

    @dvcfile.setter
    def dvcfile(self, dvcfile: Union["ProjectFile", "SingleStageFile"]) -> None:
        self._dvcfile = dvcfile

    @property
    def params(self) -> list["ParamsDependency"]:
        from dvc.dependency import ParamsDependency

        return [dep for dep in self.deps if isinstance(dep, ParamsDependency)]

    @property
    def metrics(self) -> list["Output"]:
        return [out for out in self.outs if out.metric]

    def __repr__(self):
        return f"Stage: '{self.addressing}'"

    def __str__(self):
        return f"stage: '{self.addressing}'"

    @property
    def addressing(self) -> str:
        """
        Useful for alternative presentations where we don't need
        `Stage:` prefix.
        """
        return self.relpath if self.path else "No path"

    def __hash__(self):
        return hash(self.path_in_repo)

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__
            and self.repo is other.repo
            and self.path_in_repo == other.path_in_repo
        )

    @cached_property
    def path_in_repo(self) -> str:
        return relpath(self.path, self.repo.root_dir)

    @cached_property
    def relpath(self) -> str:
        return relpath(self.path)

    @property
    def is_data_source(self) -> bool:
        """Whether the DVC file was created with `dvc add` or `dvc import`"""
        return self.cmd is None

    @property
    def is_callback(self) -> bool:
        """
        A callback stage is always considered as changed,
        so it runs on every `dvc repro` call.
        """
        return self.cmd and not any((self.deps, self.outs))

    @property
    def is_import(self) -> bool:
        """Whether the DVC file was created with `dvc import`."""
        return not self.cmd and len(self.deps) == 1 and len(self.outs) == 1

    @property
    def is_partial_import(self) -> bool:
        """
        Whether the DVC file was created using `dvc import --no-download`
        or `dvc import-url --no-download`.
        """
        return self.is_import and (not self.outs[0].hash_info)

    @property
    def is_repo_import(self) -> bool:
        if not self.is_import:
            return False

        from dvc.dependency import RepoDependency

        return isinstance(self.deps[0], RepoDependency)

    @property
    def is_db_import(self) -> bool:
        if not self.is_import:
            return False

        from dvc.dependency import DbDependency

        return isinstance(self.deps[0], DbDependency)

    @property
    def is_versioned_import(self) -> bool:
        from dvc.dependency import DbDependency

        return (
            self.is_import
            and not isinstance(self.deps[0], DbDependency)
            and self.deps[0].fs.version_aware
        )

    def short_description(self) -> Optional["str"]:
        desc: Optional[str] = None
        if self.desc:
            with suppress(ValueError):
                # try to use first non-empty line as a description
                line = next(filter(None, self.desc.splitlines()))
                return line.strip()
        return desc

    def changed_deps(
        self, allow_missing: bool = False, upstream: Optional[list] = None
    ) -> bool:
        if self.frozen:
            return False

        if self.is_callback or self.always_changed:
            return True

        return self._changed_deps(allow_missing=allow_missing, upstream=upstream)

    @rwlocked(read=["deps"])
    def _changed_deps(
        self, allow_missing: bool = False, upstream: Optional[list] = None
    ) -> bool:
        for dep in self.deps:
            status = dep.status()
            if status:
                if allow_missing and status[str(dep)] == "deleted":
                    if upstream and any(
                        dep.fs_path == out.fs_path and dep.hash_info != out.hash_info
                        for stage in upstream
                        for out in stage.outs
                    ):
                        status[str(dep)] = "modified"
                    else:
                        continue
                logger.debug(
                    "Dependency '%s' of %s changed because it is '%s'.",
                    dep,
                    self,
                    status[str(dep)],
                )
                return True
        return False

    @rwlocked(read=["outs"])
    def changed_outs(self, allow_missing: bool = False) -> bool:
        for out in self.outs:
            status = out.status()
            if status:
                if allow_missing and status[str(out)] in ["not in cache", "deleted"]:
                    continue
                logger.debug(
                    "Output '%s' of %s changed because it is '%s'.",
                    out,
                    self,
                    status[str(out)],
                )
                return True

        return False

    def changed_stage(self) -> bool:
        changed = self.md5 != self.compute_md5()
        if changed:
            logger.debug(self._changed_stage_entry())
        return changed

    @rwlocked(read=["deps", "outs"])
    def changed(
        self, allow_missing: bool = False, upstream: Optional[list] = None
    ) -> bool:
        is_changed = (
            # Short-circuit order: stage md5 is fast,
            # deps are expected to change
            self.changed_stage()
            or self.changed_deps(allow_missing=allow_missing, upstream=upstream)
            or self.changed_outs(allow_missing=allow_missing)
        )
        if is_changed:
            logger.debug("%s changed.", self)
        return is_changed

    @rwlocked(write=["outs"])
    def remove_outs(self, ignore_remove=False, force=False) -> None:
        """Used mainly for `dvc remove --outs` and :func:`Stage.reproduce`."""
        for out in self.outs:
            if out.persist and not force:
                out.unprotect()
                continue

            logger.debug("Removing output '%s' of %s.", out, self)
            out.remove(ignore_remove=ignore_remove)

    def unprotect_outs(self) -> None:
        for out in self.outs:
            out.unprotect()

    def ignore_remove_outs(self) -> None:
        for out in self.outs:
            out.ignore_remove()

    @rwlocked(write=["outs"])
    def remove(self, force=False, remove_outs=True, purge=True) -> None:
        if remove_outs:
            self.remove_outs(ignore_remove=True, force=force)
        else:
            self.unprotect_outs()
            self.ignore_remove_outs()
        if purge:
            self.dvcfile.remove_stage(self)

    def transfer(
        self,
        source: str,
        odb: Optional["ObjectDB"] = None,
        to_remote: bool = False,
        jobs: Optional[int] = None,
        force: bool = False,
    ) -> None:
        assert len(self.outs) == 1
        (out,) = self.outs
        out.transfer(source, odb=odb, jobs=jobs)
        if not to_remote:
            out.checkout(force=force)
            out.ignore()

    @rwlocked(read=["deps"], write=["outs"])
    def reproduce(self, interactive=False, **kwargs) -> Optional["Stage"]:
        force = kwargs.get("force", False)
        allow_missing = kwargs.get("allow_missing", False)
        pull = kwargs.get("pull", False)
        upstream = kwargs.pop("upstream", None)
        if force:
            pass
        # Skip stages with missing data if otherwise unchanged
        elif not self.changed(allow_missing, upstream):
            if not isinstance(self, PipelineStage) and self.is_data_source:
                logger.info("'%s' didn't change, skipping", self.addressing)
            else:
                logger.info("Stage '%s' didn't change, skipping", self.addressing)
            return None
        # Pull stages with missing data if otherwise unchanged
        elif not self.changed(True, upstream) and pull:
            try:
                logger.info("Pulling data for %s", self)
                self.repo.pull(self.addressing, jobs=kwargs.get("jobs"))
                self.checkout()
                return None
            except CheckoutError:
                logger.info("Unable to pull data for %s", self)

        msg = f"Going to reproduce {self}. Are you sure you want to continue?"
        if interactive and not prompt.confirm(msg):
            raise DvcException("reproduction aborted by the user")

        self.run(**kwargs)

        logger.debug("%s was reproduced", self)

        return self

    def update(
        self,
        rev=None,
        to_remote=False,
        remote=None,
        no_download=None,
        jobs=None,
    ) -> None:
        if not (self.is_repo_import or self.is_import):
            raise StageUpdateError(self.relpath)

        # always force update DbDep since we don't know if it's changed
        force = self.is_db_import
        update_import(
            self,
            rev=rev,
            to_remote=to_remote,
            remote=remote,
            no_download=no_download,
            jobs=jobs,
            force=force,
        )

    def reload(self) -> "Stage":
        return self.dvcfile.stage

    def dumpd(self, **kwargs) -> dict[str, Any]:
        return get_dump(self, **kwargs)

    def compute_md5(self) -> Optional[str]:
        # `dvc add`ed files don't need stage md5
        if self.is_data_source and not (self.is_import or self.is_repo_import):
            m = None
        else:
            m = compute_md5(self)
        logger.debug("Computed %s md5: '%s'", self, m)
        return m

    def save(self, allow_missing: bool = False, run_cache: bool = True):
        self.save_deps(allow_missing=allow_missing)

        self.save_outs(allow_missing=allow_missing)

        self.md5 = self.compute_md5()

        if run_cache:
            self.repo.stage_cache.save(self)

    def save_deps(self, allow_missing=False):
        from dvc.dependency.base import DependencyDoesNotExistError

        for dep in self.deps:
            try:
                dep.save()
            except DependencyDoesNotExistError:
                if not allow_missing:
                    raise

    def save_outs(self, allow_missing: bool = False):
        from dvc.output import OutputDoesNotExistError

        for out in self.outs:
            # old state just before saving so to merge them later
            old_state = out._get_versioned_meta()
            try:
                out.save()
            except OutputDoesNotExistError:
                if not allow_missing:
                    raise

            if old_state:
                out.merge_version_meta(*old_state)

    def ignore_outs(self) -> None:
        for out in self.outs:
            out.ignore()

    @staticmethod
    def _changed_entries(entries) -> list[str]:
        return [str(entry) for entry in entries if entry.workspace_status()]

    def _changed_stage_entry(self) -> str:
        return f"'md5' of {self} changed."

    def changed_entries(self) -> ChangedEntries:
        changed_deps = self._changed_entries(self.deps)
        changed_outs = self._changed_entries(self.outs)
        return (
            changed_deps,
            changed_outs,
            self._changed_stage_entry() if self.changed_stage() else None,
        )

    @rwlocked(write=["outs"])
    def commit(self, allow_missing=False, filter_info=None, **kwargs) -> None:
        from dvc.output import OutputDoesNotExistError

        link_failures = []
        for out in self.filter_outs(filter_info):
            try:
                out.commit(filter_info=filter_info, **kwargs)
            except OutputDoesNotExistError:
                if not allow_missing:
                    raise
            except CacheLinkError:
                link_failures.append(out.fs_path)
        if link_failures:
            raise CacheLinkError(link_failures)

    @rwlocked(write=["outs"])
    def add_outs(self, filter_info=None, allow_missing: bool = False, **kwargs):
        from dvc.output import OutputDoesNotExistError

        link_failures = []
        for out in self.filter_outs(filter_info):
            # old state just before saving so to merge them later
            old_state = out._get_versioned_meta()
            try:
                out.add(filter_info, **kwargs)
            except (FileNotFoundError, OutputDoesNotExistError):
                if not allow_missing:
                    raise
            except CacheLinkError:
                link_failures.append(filter_info or out.fs_path)

            if old_state:
                out.merge_version_meta(*old_state)

        if link_failures:
            raise CacheLinkError(link_failures)

    @rwlocked(read=["deps", "outs"])
    def run(
        self,
        dry=False,
        no_commit=False,
        force=False,
        allow_missing=False,
        no_download=False,
        **kwargs,
    ) -> None:
        if (self.cmd or self.is_import) and not self.frozen and not dry:
            self.remove_outs(ignore_remove=False, force=False)

        if (self.is_import and not self.frozen) or self.is_partial_import:
            self._sync_import(dry, force, kwargs.get("jobs"), no_download)
        elif not self.frozen and self.cmd:
            self._run_stage(dry, force, **kwargs)
        elif not dry:
            args = ("outputs", "frozen ") if self.frozen else ("data sources", "")
            logger.info("Verifying %s in %s%s", *args, self)
            self._check_missing_outputs()

        if not dry:
            if no_download:
                allow_missing = True

            no_cache_outs = any(
                not out.use_cache
                for out in self.outs
                if not (out.is_metric or out.is_plot)
            )
            self.save(
                allow_missing=allow_missing,
                run_cache=not no_commit and not no_cache_outs,
            )

            if no_download:
                self.ignore_outs()
            if not no_commit:
                self.commit(allow_missing=allow_missing)

    @rwlocked(read=["deps"], write=["outs"])
    def _run_stage(self, dry, force, **kwargs) -> None:
        return run_stage(self, dry, force, **kwargs)

    @rwlocked(read=["deps"], write=["outs"])
    def _sync_import(self, dry, force, jobs, no_download) -> None:
        sync_import(self, dry, force, jobs, no_download)

    @rwlocked(read=["outs"])
    def _check_missing_outputs(self) -> None:
        check_missing_outputs(self)

    def filter_outs(self, fs_path) -> Iterable["Output"]:
        def _func(o):
            return o.fs.isin_or_eq(fs_path, o.fs_path)

        return filter(_func, self.outs) if fs_path else self.outs

    @rwlocked(write=["outs"])
    def checkout(
        self, allow_missing: bool = False, **kwargs
    ) -> dict[str, list["StrPath"]]:
        stats: dict[str, list[StrPath]] = defaultdict(list)
        if self.is_partial_import:
            return stats

        for out in self.filter_outs(kwargs.get("filter_info")):
            key, outs = self._checkout(out, allow_missing=allow_missing, **kwargs)
            if key:
                stats[key].extend(outs)
        return stats

    @staticmethod
    def _checkout(out, **kwargs) -> tuple[Optional[str], list[str]]:
        try:
            result = out.checkout(**kwargs)
            added, modified = result or (None, None)
            if not (added or modified):
                return None, []
            return "modified" if modified else "added", [str(out)]
        except CheckoutError as exc:
            return "failed", exc.target_infos

    @rwlocked(read=["deps", "outs"])
    def status(
        self, check_updates: bool = False, filter_info: Optional[bool] = None
    ) -> dict[str, list[Union[str, dict[str, str]]]]:
        ret: list[Union[str, dict[str, str]]] = []
        show_import = (
            self.is_repo_import or self.is_versioned_import
        ) and check_updates

        if not self.frozen or show_import:
            self._status_deps(ret)
        self._status_outs(ret, filter_info=filter_info)
        self._status_always_changed(ret)
        self._status_stage(ret)
        return {self.addressing: ret} if ret else {}

    @staticmethod
    def _status(entries: Iterable["Output"]) -> dict[str, str]:
        ret = {}

        for entry in entries:
            ret.update(entry.status())

        return ret

    def _status_deps(self, ret) -> None:
        deps_status = self._status(self.deps)
        if deps_status:
            ret.append({"changed deps": deps_status})

    def _status_outs(self, ret, filter_info) -> None:
        filter_outs = self.filter_outs(filter_info)
        outs_status = self._status(filter_outs)
        if outs_status:
            ret.append({"changed outs": outs_status})

    def _status_always_changed(self, ret) -> None:
        if self.is_callback or self.always_changed:
            ret.append("always changed")

    def _status_stage(self, ret) -> None:
        if self.changed_stage():
            ret.append("changed checksum")

    def already_cached(self) -> bool:
        return not self.changed_stage() and self.deps_cached() and self.outs_cached()

    def deps_cached(self) -> bool:
        return all(not dep.changed() for dep in self.deps)

    def outs_cached(self) -> bool:
        return all(
            not out.changed_cache() if out.use_cache else not out.changed()
            for out in self.outs
        )

    def get_used_objs(
        self, *args, **kwargs
    ) -> dict[Optional["HashFileDB"], set["HashInfo"]]:
        """Return set of object IDs used by this stage."""
        if self.is_partial_import and not self.is_repo_import:
            return {}

        used_objs = defaultdict(set)
        for out in self.filter_outs(kwargs.get("filter_info")):
            for odb, objs in out.get_used_objs(*args, **kwargs).items():
                used_objs[odb].update(objs)
        return used_objs

    @staticmethod
    def _check_can_merge(stage, ancestor_out=None) -> None:
        if isinstance(stage, PipelineStage):
            raise MergeError("unable to auto-merge pipeline stages")

        if not stage.is_data_source or stage.deps or len(stage.outs) > 1:
            raise MergeError(
                "unable to auto-merge DVC files that weren't created by `dvc add`"
            )

        if ancestor_out and not stage.outs:
            raise MergeError("unable to auto-merge DVC files with deleted outputs")

    def merge(self, ancestor, other, allowed=None) -> None:
        assert other

        if not other.outs:
            return

        if not self.outs:
            self.outs = other.outs
            return

        if ancestor:
            self._check_can_merge(ancestor)
            outs = ancestor.outs
            ancestor_out = outs[0] if outs else None
        else:
            ancestor_out = None

        self._check_can_merge(self, ancestor_out)
        self._check_can_merge(other, ancestor_out)

        self.outs[0].merge(ancestor_out, other.outs[0], allowed=allowed)

    def dump(self, **kwargs) -> None:
        self.dvcfile.dump(self, **kwargs)


class PipelineStage(Stage):
    def __init__(self, *args, name: Optional[str] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.cmd_changed = False
        self.tracked_vars: dict[str, dict[str, dict[str, str]]] = {}

    def __eq__(self, other):
        return super().__eq__(other) and self.name == other.name

    def __hash__(self) -> int:
        return hash((self.path_in_repo, self.name))

    @property
    def addressing(self):
        from dvc.dvcfile import PROJECT_FILE

        if self.path and self.relpath == PROJECT_FILE:
            return self.name
        return f"{super().addressing}:{self.name}"

    def reload(self) -> Stage:
        from dvc.dvcfile import ProjectFile

        assert isinstance(self.dvcfile, ProjectFile)

        self.dvcfile._reset()
        return self.dvcfile.stages[self.name]

    def _status_stage(self, ret) -> None:
        if self.cmd_changed:
            ret.append("changed command")

    def changed_stage(self) -> bool:
        if self.cmd_changed:
            logger.debug(self._changed_stage_entry())
        return self.cmd_changed

    def _changed_stage_entry(self) -> str:
        return f"'cmd' of {self} has changed."

    def merge(self, ancestor, other, allowed=None):
        raise NotImplementedError
