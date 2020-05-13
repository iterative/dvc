import os

from dvc import dependency, output
from dvc.utils.fs import path_isin

from .exceptions import (
    StagePathNotDirectoryError,
    StagePathNotFoundError,
    StagePathOutsideError,
)
from .params import OutputParams


def check_stage_path(repo, path, is_wdir=False):
    assert repo is not None

    error_msg = "{wdir_or_path} '{path}' {{}}".format(
        wdir_or_path="stage working dir" if is_wdir else "file path",
        path=path,
    )

    real_path = os.path.realpath(path)
    if not os.path.exists(real_path):
        raise StagePathNotFoundError(error_msg.format("does not exist"))

    if not os.path.isdir(real_path):
        raise StagePathNotDirectoryError(error_msg.format("is not directory"))

    proj_dir = os.path.realpath(repo.root_dir)
    if real_path != proj_dir and not path_isin(real_path, proj_dir):
        raise StagePathOutsideError(error_msg.format("is outside of DVC repo"))


def fill_stage_outputs(stage, **kwargs):
    assert not stage.outs

    stage.outs = []
    for key in (p.value for p in OutputParams):
        stage.outs += output.loads_from(
            stage,
            kwargs.get(key, []),
            use_cache="no_cache" not in key,
            persist="persist" in key,
            metric="metrics" in key,
        )


def fill_stage_dependencies(stage, deps=None, erepo=None, params=None):
    assert not stage.deps
    stage.deps = []
    stage.deps += dependency.loads_from(stage, deps or [], erepo=erepo)
    stage.deps += dependency.loads_params(stage, params or [])


def check_circular_dependency(stage):
    from dvc.exceptions import CircularDependencyError

    circular_dependencies = set(d.path_info for d in stage.deps) & set(
        o.path_info for o in stage.outs
    )

    if circular_dependencies:
        raise CircularDependencyError(str(circular_dependencies.pop()))


def check_duplicated_arguments(stage):
    from dvc.exceptions import ArgumentDuplicationError
    from collections import Counter

    path_counts = Counter(edge.path_info for edge in stage.deps + stage.outs)

    for path, occurrence in path_counts.items():
        if occurrence > 1:
            raise ArgumentDuplicationError(str(path))
