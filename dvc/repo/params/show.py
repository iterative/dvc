import logging

from dvc.dependency.param import ParamsDependency
from dvc.exceptions import DvcException
from dvc.path_info import PathInfo
from dvc.repo import locked
from dvc.utils.serialize import PARSERS, ParseError

logger = logging.getLogger(__name__)


class NoParamsError(DvcException):
    pass


def _collect_configs(repo):
    configs = set()
    configs.add(PathInfo(repo.root_dir) / ParamsDependency.DEFAULT_PARAMS_FILE)
    for stage in repo.stages:
        for dep in stage.deps:
            if not isinstance(dep, ParamsDependency):
                continue

            configs.add(dep.path_info)
    return list(configs)


def _read_params(repo, configs, rev):
    res = {}
    for config in configs:
        if not repo.tree.exists(config):
            continue

        suffix = config.suffix.lower()
        parser = PARSERS[suffix]
        with repo.tree.open(config, "r") as fobj:
            try:
                res[str(config)] = parser(fobj.read(), config)
            except ParseError:
                logger.debug(
                    "failed to read '%s' on '%s'", config, rev, exc_info=True
                )
                continue

    return res


@locked
def show(repo, revs=None):
    res = {}

    for branch in repo.brancher(revs=revs):
        configs = _collect_configs(repo)
        params = _read_params(repo, configs, branch)

        if params:
            res[branch] = params

    if not res:
        raise NoParamsError("no parameter configs files in this repository")

    # Hide workspace params if they are the same as in the active branch
    try:
        active_branch = repo.scm.active_branch()
    except TypeError:
        pass  # Detached head
    else:
        if res.get("workspace") == res.get(active_branch):
            res.pop("workspace", None)

    return res
