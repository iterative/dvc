import os
from collections import defaultdict
from collections.abc import Iterator
from itertools import chain
from typing import TYPE_CHECKING, Any, Optional, Union

from dvc.dependency.param import ParamsDependency, read_param_file
from dvc.log import logger
from dvc.repo.metrics.show import FileResult, Result, try_expand_paths
from dvc.stage import PipelineStage
from dvc.utils import as_posix
from dvc.utils.collections import ensure_list

if TYPE_CHECKING:
    from dvc.fs import FileSystem
    from dvc.repo import Repo

logger = logger.getChild(__name__)


def _collect_top_level_params(repo: "Repo") -> Iterator[str]:
    top_params = repo.index._params
    for dvcfile, params in top_params.items():
        wdir = repo.fs.relpath(repo.fs.parent(dvcfile), repo.root_dir)
        for file in params:
            path = repo.fs.join(wdir, as_posix(file))
            yield repo.fs.normpath(path)


def params_from_target(
    repo: "Repo", targets: list[str]
) -> Iterator["ParamsDependency"]:
    stages = chain.from_iterable(repo.stage.collect(target) for target in targets)
    for stage in stages:
        yield from stage.params


def _collect_params(
    repo: "Repo",
    targets: Union[list[str], dict[str, list[str]], None] = None,
    stages: Optional[list[str]] = None,
    deps_only: bool = False,
    default_file: Optional[str] = None,
) -> dict[str, list[str]]:
    from dvc.dependency import _merge_params

    if isinstance(targets, list):
        targets = {target: [] for target in targets}

    params: list[dict[str, list[str]]] = []

    if targets:
        # target is a repo-relative path
        params.extend({file: params} for file, params in targets.items())

    if not targets or stages:
        deps = params_from_target(repo, stages) if stages else repo.index.params
        relpath = repo.fs.relpath
        params.extend(
            {relpath(dep.fs_path, repo.root_dir): list(dep.params)} for dep in deps
        )

    fs = repo.dvcfs

    if not targets and not deps_only and not stages:
        # _collect_top_level_params returns repo-relative paths
        params.extend({param: []} for param in _collect_top_level_params(repo))
        if default_file and fs.exists(f"{fs.root_marker}{default_file}"):
            params.append({default_file: []})

    # combine all the param files and the keypaths to track
    all_params = _merge_params(params)

    ret = {}
    for param, _params in all_params.items():
        # convert to posixpath for DVCFileSystem
        path = fs.from_os_path(param)
        # make paths absolute for DVCFileSystem
        repo_path = f"{fs.root_marker}{path}"
        ret.update(dict.fromkeys(try_expand_paths(fs, [repo_path]), _params))
    return ret


def _collect_vars(repo, params, stages=None) -> dict:
    vars_params: dict[str, dict] = defaultdict(dict)

    for stage in repo.index.stages:
        if isinstance(stage, PipelineStage) and stage.tracked_vars:
            if stages and stage.addressing not in stages:
                continue
            for file, vars_ in stage.tracked_vars.items():
                # `params` file are shown regardless of `tracked` or not
                # to reduce noise and duplication, they are skipped

                # `file` is relative
                abspath = repo.fs.abspath(file)
                repo_path = repo.dvcfs.from_os_path(abspath)
                if repo_path in params:
                    continue

                vars_params[repo_path].update(vars_)
    return dict(vars_params)


def _read_params(
    fs: "FileSystem", params: dict[str, list[str]], **load_kwargs
) -> Iterator[tuple[str, Union[Exception, Any]]]:
    for file_path, key_paths in params.items():
        try:
            yield file_path, read_param_file(fs, file_path, key_paths, **load_kwargs)
        except Exception as exc:  # noqa: BLE001
            logger.debug(exc)
            yield file_path, exc


def _gather_params(
    repo: "Repo",
    targets: Union[list[str], dict[str, list[str]], None] = None,
    deps_only: bool = False,
    stages: Optional[list[str]] = None,
    on_error: str = "return",
):
    assert on_error in ("raise", "return", "ignore")

    # `files` is a repo-relative posixpath that can be passed to DVCFileSystem
    # It is absolute, i.e. has a root_marker `/` in front which we strip when returning
    # the result and convert to appropriate repo-relative os.path.
    files_keypaths = _collect_params(
        repo,
        targets=targets,
        stages=stages,
        deps_only=deps_only,
        default_file=ParamsDependency.DEFAULT_PARAMS_FILE,
    )

    data: dict[str, FileResult] = {}

    fs = repo.dvcfs
    for fs_path, result in _read_params(fs, files_keypaths, cache=True):
        repo_path = fs_path.lstrip(fs.root_marker)
        repo_os_path = os.sep.join(fs.parts(repo_path))
        if not isinstance(result, Exception):
            data.update({repo_os_path: FileResult(data=result)})
            continue

        if on_error == "raise":
            raise result
        if on_error == "return":
            data.update({repo_os_path: FileResult(error=result)})

    if not (stages or targets):
        data.update(
            {
                path: FileResult(data=result)
                for path, result in _collect_vars(repo, data).items()
            }
        )
    return data


def show(
    repo: "Repo",
    targets: Optional[list[str]] = None,
    stages: Optional[list[str]] = None,
    deps_only: bool = False,
    all_branches: bool = False,
    all_tags: bool = False,
    revs: Optional[list[str]] = None,
    all_commits: bool = False,
    hide_workspace: bool = True,
    on_error: str = "return",
) -> dict[str, Result]:
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
        except Exception as exc:
            if on_error == "raise":
                raise
            logger.warning("failed to load params in revision %r, %s", rev, str(exc))
            if on_error == "return":
                res[rev] = Result(error=exc)

    if hide_workspace:
        from dvc.repo.metrics.show import _hide_workspace

        _hide_workspace(repo.scm, res)
    return res
