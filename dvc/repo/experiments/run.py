import logging
from typing import Iterable, Optional

from dvc.exceptions import InvalidArgumentError
from dvc.repo import locked
from dvc.repo.experiments import UnchangedExperimentError

logger = logging.getLogger(__name__)


def _parse_params(path_params: Iterable):
    from ruamel.yaml import YAMLError

    from dvc.dependency.param import ParamsDependency
    from dvc.utils.serialize import loads_yaml

    ret = {}
    for path_param in path_params:
        path, _, params_str = path_param.rpartition(":")
        # remove empty strings from params, on condition such as `-p "file1:"`
        params = {}
        for param_str in filter(bool, params_str.split(",")):
            try:
                # interpret value strings using YAML rules
                key, value = param_str.split("=")
                params[key] = loads_yaml(value)
            except (ValueError, YAMLError):
                raise InvalidArgumentError(
                    f"Invalid param/value pair '{param_str}'"
                )
        if not path:
            path = ParamsDependency.DEFAULT_PARAMS_FILE
        ret[path] = params
    return ret


@locked
def run(
    repo,
    target: Optional[str] = None,
    params: Optional[Iterable] = None,
    run_all: Optional[bool] = False,
    jobs: Optional[int] = 1,
    **kwargs,
) -> dict:
    """Reproduce the specified target as an experiment.

    Accepts the same additional kwargs as Repo.reproduce.

    Returns a dict mapping new experiment SHAs to the results
    of `repro` for that experiment.
    """
    if run_all:
        return repo.experiments.reproduce_queued(jobs=jobs)

    if params:
        params = _parse_params(params)
    else:
        params = []
    try:
        return repo.experiments.reproduce_one(
            target=target, params=params, **kwargs
        )
    except UnchangedExperimentError:
        # If experiment contains no changes, just run regular repro
        return {None: repo.reproduce(target=target, **kwargs)}
