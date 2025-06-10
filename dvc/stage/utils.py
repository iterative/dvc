import os
import pathlib
from typing import TYPE_CHECKING, Any, Optional, Union

from funcy import concat, first, lsplit, rpartial

from dvc.annotations import ANNOTATION_FIELDS
from dvc.exceptions import InvalidArgumentError
from dvc_data.hashfile.meta import Meta

from .exceptions import (
    MissingDataSource,
    StageExternalOutputsError,
    StagePathNotDirectoryError,
    StagePathNotFoundError,
    StagePathOutsideError,
)

if TYPE_CHECKING:
    from dvc.dependency import Dependency, ParamsDependency
    from dvc.repo import Repo

    from . import PipelineStage, Stage


def check_stage_path(repo, path, is_wdir=False):
    from dvc.utils.fs import path_isin

    assert repo is not None

    error_msg = "{wdir_or_path} '{path}' {{}}".format(
        wdir_or_path="stage working dir" if is_wdir else "file path", path=path
    )

    real_path = os.path.abspath(path)
    if not os.path.exists(real_path):
        raise StagePathNotFoundError(error_msg.format("does not exist"))

    if not os.path.isdir(real_path):
        raise StagePathNotDirectoryError(error_msg.format("is not directory"))

    proj_dir = os.path.abspath(repo.root_dir)
    if real_path != proj_dir and not path_isin(real_path, proj_dir):
        raise StagePathOutsideError(error_msg.format("is outside of DVC repo"))


def fill_stage_outputs(stage, **kwargs):
    from dvc.output import loads_from

    assert not stage.outs

    keys = [
        "outs_persist",
        "outs_persist_no_cache",
        "metrics",
        "metrics_persist",
        "metrics_no_cache",
        "metrics_persist_no_cache",
        "plots",
        "plots_persist",
        "plots_no_cache",
        "plots_persist_no_cache",
        "outs_no_cache",
        "outs",
    ]

    stage.outs = []

    for key in keys:
        stage.outs += loads_from(
            stage,
            kwargs.get(key, []),
            use_cache="no_cache" not in key,
            persist="persist" in key,
            metric="metrics" in key,
            plot="plots" in key,
        )


def fill_stage_dependencies(
    stage, deps=None, erepo=None, params=None, fs_config=None, db=None
):
    from dvc.dependency import loads_from, loads_params

    assert not stage.deps
    stage.deps = []
    stage.deps += loads_from(stage, deps or [], erepo=erepo, fs_config=fs_config, db=db)
    stage.deps += loads_params(stage, params or [])


def check_no_externals(stage):
    from dvc.utils import format_link

    def _is_cached_external(out):
        return not out.is_in_repo and out.use_cache

    outs = [str(out) for out in stage.outs if _is_cached_external(out)]
    if not outs:
        return

    str_outs = ", ".join(outs)
    link = format_link(
        "https://dvc.org/doc/user-guide/pipelines/external-dependencies-and-outputs"
    )
    if stage.is_data_source:
        link = format_link(
            "https://dvc.org/doc/user-guide/data-management/importing-external-data"
        )
    raise StageExternalOutputsError(
        f"Cached output(s) outside of DVC project: {str_outs}. "
        f"See {link} for more info."
    )


def check_circular_dependency(stage):
    from dvc.exceptions import CircularDependencyError

    circular_dependencies = {d.fs_path for d in stage.deps} & {
        o.fs_path for o in stage.outs
    }

    if circular_dependencies:
        raise CircularDependencyError(str(circular_dependencies.pop()))


def check_duplicated_arguments(stage):
    from collections import Counter

    from dvc.exceptions import ArgumentDuplicationError

    path_counts = Counter(edge.fs_path for edge in stage.deps + stage.outs)

    for path, occurrence in path_counts.items():
        if occurrence > 1:
            raise ArgumentDuplicationError(str(path))


def check_missing_outputs(stage):
    paths = [str(out) for out in stage.outs if not out.exists]
    if paths:
        raise MissingDataSource(paths)


def compute_md5(stage):
    from dvc.output import Output
    from dvc.utils import dict_md5

    d = stage.dumpd()

    # Remove md5 and meta, these should not affect stage md5
    d.pop(stage.PARAM_MD5, None)
    d.pop(stage.PARAM_META, None)
    d.pop(stage.PARAM_DESC, None)

    # Ignore the wdir default value. In this case DVC file w/o
    # wdir has the same md5 as a file with the default value specified.
    # It's important for backward compatibility with pipelines that
    # didn't have WDIR in their DVC files.
    if d.get(stage.PARAM_WDIR) == ".":
        del d[stage.PARAM_WDIR]

    return dict_md5(
        d,
        exclude=[
            *ANNOTATION_FIELDS,
            stage.PARAM_LOCKED,  # backward compatibility
            stage.PARAM_FROZEN,
            Output.PARAM_METRIC,
            Output.PARAM_PERSIST,
            Meta.PARAM_ISEXEC,
            Meta.PARAM_SIZE,
            Meta.PARAM_NFILES,
        ],
    )


def resolve_wdir(wdir, path):
    from dvc.utils import relpath

    rel_wdir = relpath(wdir, os.path.dirname(path))
    return pathlib.PurePath(rel_wdir).as_posix() if rel_wdir != "." else None


def resolve_paths(fs, path, wdir=None):
    path = fs.abspath(path)
    wdir = wdir or os.curdir
    wdir = fs.abspath(fs.join(fs.dirname(path), wdir))
    return path, wdir


def get_dump(stage: "Stage", **kwargs):
    return {
        key: value
        for key, value in {
            stage.PARAM_DESC: stage.desc,
            stage.PARAM_MD5: stage.md5,
            stage.PARAM_CMD: stage.cmd,
            stage.PARAM_WDIR: resolve_wdir(stage.wdir, stage.path),
            stage.PARAM_FROZEN: stage.frozen,
            stage.PARAM_DEPS: [d.dumpd(**kwargs) for d in stage.deps],
            stage.PARAM_OUTS: [o.dumpd(**kwargs) for o in stage.outs],
            stage.PARAM_ALWAYS_CHANGED: stage.always_changed,
            stage.PARAM_META: stage.meta,
        }.items()
        if value
    }


def split_params_deps(
    stage: "Stage",
) -> tuple[list["ParamsDependency"], list["Dependency"]]:
    from dvc.dependency import ParamsDependency

    return lsplit(rpartial(isinstance, ParamsDependency), stage.deps)


def is_valid_name(name: str) -> bool:
    from . import INVALID_STAGENAME_CHARS

    return not INVALID_STAGENAME_CHARS & set(name)


def prepare_file_path(kwargs) -> str:
    """Determine file path from the first output name.

    Used in creating .dvc files.
    """
    from dvc.dvcfile import DVC_FILE_SUFFIX

    out = first(
        concat(
            kwargs.get("outs", []),
            kwargs.get("outs_no_cache", []),
            kwargs.get("metrics", []),
            kwargs.get("metrics_no_cache", []),
            kwargs.get("plots", []),
            kwargs.get("plots_no_cache", []),
            kwargs.get("outs_persist", []),
            kwargs.get("outs_persist_no_cache", []),
        )
    )
    assert out
    return os.path.basename(os.path.normpath(out)) + DVC_FILE_SUFFIX


def check_stage_exists(repo: "Repo", stage: Union["Stage", "PipelineStage"], path: str):
    from dvc.dvcfile import load_file
    from dvc.stage import PipelineStage
    from dvc.stage.exceptions import DuplicateStageName, StageFileAlreadyExistsError

    dvcfile = load_file(repo, path)
    if not dvcfile.exists():
        return

    hint = "Use '--force' to overwrite."
    if not isinstance(stage, PipelineStage):
        raise StageFileAlreadyExistsError(f"'{stage.relpath}' already exists. {hint}")
    if stage.name and stage.name in dvcfile.stages:
        raise DuplicateStageName(
            f"Stage '{stage.name}' already exists in '{stage.relpath}'. {hint}"
        )


def validate_kwargs(
    single_stage: bool = False, fname: Optional[str] = None, **kwargs
) -> dict[str, Any]:
    """Prepare, validate and process kwargs passed from cli"""
    cmd = kwargs.get("cmd")
    if not cmd and not single_stage:
        raise InvalidArgumentError("command is not specified")

    stage_name = kwargs.get("name")
    if stage_name and single_stage:
        raise InvalidArgumentError("`-n|--name` is incompatible with `--single-stage`")
    if stage_name and fname:
        raise InvalidArgumentError(
            "`--file` is currently incompatible with `-n|--name` "
            "and requires `--single-stage`"
        )
    if not stage_name and not single_stage:
        raise InvalidArgumentError("`-n|--name` is required")

    if single_stage:
        kwargs.pop("name", None)

    return kwargs


def _get_stage_files(stage: "Stage") -> list[str]:
    from dvc.dvcfile import ProjectFile
    from dvc.utils import relpath

    ret: list[str] = []
    file = stage.dvcfile
    ret.append(file.relpath)
    if isinstance(file, ProjectFile):
        ret.append(file._lockfile.relpath)

    for dep in stage.deps:
        if (
            not dep.use_scm_ignore
            and dep.is_in_repo
            and not stage.repo.dvcfs.isdvc(stage.repo.dvcfs.from_os_path(str(dep)))
        ):
            ret.append(relpath(dep.fs_path))  # noqa: PERF401

    for out in stage.outs:
        if not out.use_scm_ignore and out.is_in_repo:
            ret.append(relpath(out.fs_path))  # noqa: PERF401
    return ret
