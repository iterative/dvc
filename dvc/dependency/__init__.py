from collections import defaultdict
from typing import Any, Mapping

from dvc.fs import get_cloud_fs
from dvc.output import ARTIFACT_SCHEMA, Output

from .base import Dependency
from .param import ParamsDependency
from .repo import RepoDependency
from .versioned import VersionedDependency

# NOTE: schema for dependencies is basically the same as for outputs, but
# without output-specific entries like 'cache' (whether or not output is
# cached, see -o and -O flags for `dvc run`) and 'metric' (whether or not
# output is a metrics file and how to parse it, see `-M` flag for `dvc run`).
SCHEMA: Mapping[str, Any] = {
    **ARTIFACT_SCHEMA,
    **RepoDependency.REPO_SCHEMA,
    **ParamsDependency.PARAM_SCHEMA,
}


def _get(stage, p, info):
    if info and info.get(RepoDependency.PARAM_REPO):
        repo = info.pop(RepoDependency.PARAM_REPO)
        return RepoDependency(repo, stage, p, info)

    if info and info.get(ParamsDependency.PARAM_PARAMS):
        params = info.pop(ParamsDependency.PARAM_PARAMS)
        return ParamsDependency(stage, p, params)

    _, fs_config, _ = get_cloud_fs(stage.repo, url=p)
    if fs_config.get("version_aware") or info.get(Output.PARAM_VERSION_ID):
        version_id = info.pop("version_id", None)
        return VersionedDependency(stage, p, info=info, version_id=version_id)
    return Dependency(stage, p, info)


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(Output.PARAM_PATH, None)
        ret.append(_get(stage, p, d))
    return ret


def loads_from(stage, s_list, erepo=None):
    assert isinstance(s_list, list)
    info = {RepoDependency.PARAM_REPO: erepo} if erepo else {}
    return [_get(stage, s, info.copy()) for s in s_list]


def _merge_params(s_list):
    d = defaultdict(list)
    default_file = ParamsDependency.DEFAULT_PARAMS_FILE

    # figure out completely tracked params file, and ignore specific keys
    wholly_tracked = set()
    for key in s_list:
        if not isinstance(key, dict):
            continue
        wholly_tracked.update(k for k, params in key.items() if not params)

    for key in s_list:
        if isinstance(key, str):
            if default_file not in wholly_tracked:
                d[default_file].append(key)
            continue

        if not isinstance(key, dict):
            msg = "Only list of str/dict is supported. Got: "
            msg += f"'{type(key).__name__}'."
            raise ValueError(msg)

        for k, params in key.items():
            if k in wholly_tracked:
                d[k] = []
                continue
            if not isinstance(params, list):
                msg = "Expected list of params for custom params file "
                msg += f"'{k}', got '{type(params).__name__}'."
                raise ValueError(msg)
            d[k].extend(params)
    return d


def loads_params(stage, s_list):
    d = _merge_params(s_list)
    return [
        ParamsDependency(stage, path, params) for path, params in d.items()
    ]
