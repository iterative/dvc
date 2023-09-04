import logging
import os
from collections import defaultdict
from itertools import chain
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple, Union

from dvc.dependency.param import ParamsDependency
from dvc.repo import locked
from dvc.repo.metrics.show import FileResult, Result
from dvc.stage import PipelineStage
from dvc.utils import as_posix, expand_paths
from dvc.utils.collections import ensure_list
from dvc.utils.serialize import load_path

if TYPE_CHECKING:
    from dvc.fs import FileSystem
    from dvc.repo import Repo

logger = logging.getLogger(__name__)


def _collect_top_level_params(repo: "Repo") -> Iterator[str]:
    top_params = repo.index._params  # pylint: disable=protected-access
    for dvcfile, params in top_params.items():
        wdir = repo.fs.path.relpath(repo.fs.path.parent(dvcfile), repo.root_dir)
        for file in params:
            path = repo.fs.path.join(wdir, as_posix(file))
            yield repo.fs.path.normpath(path)


def _collect_params(
    repo: "Repo",
    targets: Union[List[str], Dict[str, List[str]], None] = None,
    stages: Optional[List[str]] = None,
    deps_only: bool = False,
    default_file: Optional[str] = None,
) -> Dict[str, List[str]]:
    dvcfs = repo.dvcfs
    if isinstance(targets, list):
        targets = {target: [] for target in targets}

    params = {}
    if targets and not deps_only:
        params.update(targets)

    if not params or stages:
        from dvc.dependency import _merge_params

        param_deps = params_from_target(repo, stages) if stages else repo.index.params
        params.update(
            _merge_params(
                [
                    {dvcfs.from_os_path(dep.fs_path): list(dep.params)}
                    for dep in param_deps
                ]
            )
        )

    if not targets and not deps_only and not stages:
        params.update({param: [] for param in _collect_top_level_params(repo)})
        if default_file and repo.dvcfs.exists(os.sep + default_file):
            params.update({default_file: []})

    ret = {}
    for param_file, _params in params.items():
        path = f"{os.sep}{param_file}"
        ret.update({file: _params for file in expand_paths(dvcfs, [path])})

    return ret


def _collect_vars(repo, params, stages=None) -> Dict:
    vars_params: Dict[str, Dict] = defaultdict(dict)

    for stage in repo.index.stages:
        if isinstance(stage, PipelineStage) and stage.tracked_vars:
            if stages and stage.addressing not in stages:
                continue
            for file, vars_ in stage.tracked_vars.items():
                # `params` file are shown regardless of `tracked` or not
                # to reduce noise and duplication, they are skipped
                if file in params:
                    continue

                name = os.sep.join(repo.fs.path.parts(file))
                vars_params[name].update(vars_)
    return vars_params


def _read_param(
    fs: "FileSystem", path: str, key_paths: Optional[List[str]] = None
) -> Any:
    import dpath

    config = load_path(path, fs)
    if not key_paths:
        return config

    ret: Dict = {}
    from dpath import merge

    for key_path in key_paths:
        merge(
            ret,
            dpath.search(config, key_path, separator="."),
            separator=".",
        )
    return ret


def _read_params(
    fs: "FileSystem", params: Dict[str, List[str]]
) -> Iterator[Tuple[str, Union[Exception, Any]]]:
    for file_path, key_paths in params.items():
        try:
            yield file_path, _read_param(fs, file_path, key_paths)
        except Exception as exc:  # noqa: BLE001 # pylint:disable=broad-exception-caught
            logger.debug(exc)
            yield file_path, exc


def params_from_target(
    repo: "Repo", targets: List[str]
) -> Iterator["ParamsDependency"]:
    stages = chain.from_iterable(repo.stage.collect(target) for target in targets)
    for stage in stages:
        yield from stage.param_deps


def _gather_params(
    repo: "Repo",
    targets: Union[List[str], Dict[str, List[str]], None] = None,
    deps_only: bool = False,
    stages: Optional[List[str]] = None,
    on_error: str = "return",
):
    assert on_error in ("raise", "return", "ignore")

    files_keypaths = _collect_params(
        repo,
        targets=targets,
        stages=stages,
        deps_only=deps_only,
        default_file=ParamsDependency.DEFAULT_PARAMS_FILE,
    )

    data: Dict[str, FileResult] = {}
    for file, result in _read_params(repo.dvcfs, files_keypaths):
        repo_path = file.lstrip(os.sep)
        if not isinstance(result, Exception):
            data.update({repo_path: FileResult(data=result)})
            continue

        if on_error == "raise":
            raise result
        if on_error == "return":
            data.update({repo_path: FileResult(error=result)})

    # vars_params = _collect_vars(repo, params, stages=stages)

    # NOTE: only those that are not added as a ParamDependency are
    # included so we don't need to recursively merge them yet.
    # for key, vals in vars_params.items():
    #     params[key]["data"] = vals
    return data


@locked
def show(
    repo: "Repo",
    targets: Optional[List[str]] = None,
    stages: Optional[List[str]] = None,
    deps_only: bool = False,
    all_branches: bool = False,
    all_tags: bool = False,
    revs: Optional[List[str]] = None,
    all_commits: bool = False,
    hide_workspace: bool = True,
    on_error: str = "return",
) -> Dict[str, Result]:
    assert on_error in ("raise", "return", "ignore")
    res = {}

    targets = ensure_list(targets)
    targets = [repo.dvcfs.from_os_path(target) for target in targets]

    for rev in repo.brancher(
        revs=revs,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
    ):
        try:
            params = _gather_params(
                repo=repo,
                targets=targets,
                stages=stages,
                deps_only=deps_only,
                on_error=on_error,
            )
            res[rev] = Result(data=params)
        except Exception as exc:  # noqa: BLE001 # pylint:disable=broad-exception-caught
            if on_error == "raise":
                raise
            logger.warning("failed to load params in revision %r, %s", rev, str(exc))
            if on_error == "return":
                res[rev] = Result(error=exc)

    if hide_workspace:
        from dvc.repo.metrics.show import _hide_workspace

        _hide_workspace(repo.scm, res)
    return res
