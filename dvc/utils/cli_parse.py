from collections import defaultdict
from typing import Dict, Iterable, List


def parse_params(path_params: Iterable[str]) -> List[Dict[str, List[str]]]:
    """Normalizes the shape of params from the CLI to dict."""
    from dvc.dependency.param import ParamsDependency

    ret: Dict[str, List[str]] = defaultdict(list)
    for path_param in path_params:
        path, _, params_str = path_param.rpartition(":")
        # remove empty strings from params, on condition such as `-p "file1:"`
        params = filter(bool, params_str.split(","))
        if not path:
            path = ParamsDependency.DEFAULT_PARAMS_FILE
        ret[path].extend(params)
    return [{path: params} for path, params in ret.items()]


def to_path_overrides(
    path_params: Iterable[str],
) -> Dict[str, List[str]]:
    """Group overrides by path"""
    from dvc.dependency.param import ParamsDependency

    path_overrides = defaultdict(list)
    for path_param in path_params:
        path_and_name = path_param.partition("=")[0]
        if ":" not in path_and_name:
            override = path_param
            path = ParamsDependency.DEFAULT_PARAMS_FILE
        else:
            path, _, override = path_param.partition(":")

        path_overrides[path].append(override)

    return dict(path_overrides)
