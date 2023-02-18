import logging
import os
from collections import defaultdict
from copy import copy
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Optional, Tuple

from scmrepo.exceptions import SCMError

from dvc.dependency.param import ParamsDependency
from dvc.repo import locked
from dvc.repo.collect import collect
from dvc.scm import NoSCMError
from dvc.stage import PipelineStage
from dvc.ui import ui
from dvc.utils import as_posix, error_handler, errored_revisions, onerror_collect
from dvc.utils.collections import ensure_list
from dvc.utils.serialize import load_path

if TYPE_CHECKING:
    from dvc.output import Output
    from dvc.repo import Repo

logger = logging.getLogger(__name__)


def _is_params(dep: "Output"):
    return isinstance(dep, ParamsDependency)


def _collect_top_level_params(repo):
    top_params = repo.index._params  # pylint: disable=protected-access
    for dvcfile, params in top_params.items():
        wdir = repo.fs.path.relpath(repo.fs.path.parent(dvcfile), repo.root_dir)
        for file in params:
            path = repo.fs.path.join(wdir, as_posix(file))
            yield repo.fs.path.normpath(path)


def _collect_configs(
    repo: "Repo", targets=None, deps=False, stages=None
) -> Tuple[List["Output"], List[str]]:
    params, fs_paths = collect(
        repo,
        targets=targets or [],
        deps=True,
        output_filter=_is_params,
        duplicates=deps or stages is not None,
    )
    all_fs_paths = fs_paths + [p.fs_path for p in params]
    if not any([deps, targets, stages]):
        default_params = repo.fs.path.join(
            repo.root_dir, ParamsDependency.DEFAULT_PARAMS_FILE
        )
        if default_params not in all_fs_paths and repo.fs.exists(default_params):
            fs_paths.append(default_params)
    if targets and (deps or stages) and not params:
        # A target has been provided but it is not used in the stages
        fs_paths = []
    return params, fs_paths


@error_handler
def _read_fs_path(fs, fs_path, **kwargs):
    return load_path(fs_path, fs)


def _read_params(
    repo,
    params,
    params_fs_paths,
    deps=False,
    onerror: Optional[Callable] = None,
    stages: Optional[Iterable[str]] = None,
):
    res: Dict[str, Dict] = defaultdict(lambda: defaultdict(dict))
    fs_paths = copy(params_fs_paths)

    if deps or stages:
        for param in params:
            if stages and param.stage.addressing not in stages:
                continue
            params_dict = error_handler(param.read_params)(
                onerror=onerror, flatten=False
            )
            if params_dict:
                name = os.sep.join(repo.fs.path.relparts(param.fs_path))
                res[name]["data"].update(params_dict["data"])
                if name in fs_paths:
                    fs_paths.remove(name)
    else:
        fs_paths += [param.fs_path for param in params]

    for fs_path in fs_paths:
        from_path = _read_fs_path(repo.fs, fs_path, onerror=onerror)
        if from_path:
            name = os.sep.join(repo.fs.path.relparts(fs_path))
            res[name] = from_path

    return res


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


@locked
def show(
    repo,
    revs=None,
    targets=None,
    deps=False,
    onerror: Optional[Callable] = None,
    stages=None,
    hide_workspace=True,
):
    if onerror is None:
        onerror = onerror_collect
    res = {}

    targets = ensure_list(targets)
    targets = [repo.dvcfs.from_os_path(target) for target in targets]

    for branch in repo.brancher(revs=revs):
        params = error_handler(_gather_params)(
            repo=repo,
            targets=targets,
            deps=deps,
            onerror=onerror,
            stages=stages,
        )

        if params:
            res[branch] = params

    if hide_workspace:
        # Hide workspace params if they are the same as in the active branch
        try:
            active_branch = repo.scm.active_branch()
        except (SCMError, NoSCMError):
            # SCMError - detached head
            # NoSCMError - no repo case
            pass
        else:
            if res.get("workspace") == res.get(active_branch):
                res.pop("workspace", None)

    errored = errored_revisions(res)
    if errored:
        ui.error_write(
            "DVC failed to load some parameters for following revisions:"
            f" '{', '.join(errored)}'."
        )

    return res


def _gather_params(repo, targets=None, deps=False, onerror=None, stages=None):
    param_outs, params_fs_paths = _collect_configs(
        repo, targets=targets, deps=deps, stages=stages
    )
    params_fs_paths.extend(_collect_top_level_params(repo=repo))
    params = _read_params(
        repo,
        params=param_outs,
        params_fs_paths=params_fs_paths,
        deps=deps,
        onerror=onerror,
        stages=stages,
    )
    vars_params = _collect_vars(repo, params, stages=stages)

    # NOTE: only those that are not added as a ParamDependency are
    # included so we don't need to recursively merge them yet.
    for key, vals in vars_params.items():
        params[key]["data"] = vals
    return params
