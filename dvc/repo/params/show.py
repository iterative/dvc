import copy

from dvc.repo import locked
from dvc.exceptions import DvcException
from dvc.dependency.param import DependencyPARAMS


class NoParamsError(DvcException):
    pass


def _collect_params(repo):
    configs = {}
    for stage in repo.stages:
        for dep in stage.deps:
            if not isinstance(dep, DependencyPARAMS):
                continue

            if dep.path_info not in configs.keys():
                configs[dep.path_info] = copy.copy(dep)
                continue

            params = set(configs[dep.path_info].params)
            params.update(set(dep.params))
            configs[dep.path_info].params = list(params)

    return configs.values()


def _read_params(deps):
    res = {}
    for dep in deps:
        assert dep.scheme == "local"

        params = dep.read_params()
        if not params:
            continue

        res[str(dep.path_info)] = params

    return res


@locked
def show(repo, revs=None):
    res = {}

    for branch in repo.brancher(revs=revs):
        entries = _collect_params(repo)
        params = _read_params(entries)

        if params:
            res[branch] = params

    if not res:
        raise NoParamsError("no parameter configs files in this repository")

    # Hide working tree params if they are the same as in the active branch
    try:
        active_branch = repo.scm.active_branch()
    except TypeError:
        pass  # Detached head
    else:
        if res.get("working tree") == res.get(active_branch):
            res.pop("working tree", None)

    return res
