from collections import defaultdict
from typing import Any, Dict, Iterable, List


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


def loads_params(path_params: Iterable[str],) -> Dict[str, Dict[str, Any]]:

    """Loads the content of params from the cli as Python object."""
    from ruamel.yaml import YAMLError

    from dvc.exceptions import InvalidArgumentError

    from .serialize import loads_yaml

    normalized_params = parse_params(path_params)
    ret: Dict[str, Dict[str, Any]] = defaultdict(dict)

    for part in normalized_params:
        assert part
        (item,) = part.items()
        path, param_keys = item

        for param_str in param_keys:
            try:
                key, _, value = param_str.partition("=")
                # interpret value strings using YAML rules
                parsed = loads_yaml(value)
                ret[path][key] = parsed
            except (ValueError, YAMLError):
                raise InvalidArgumentError(
                    f"Invalid param/value pair '{param_str}'"
                )
    return ret
