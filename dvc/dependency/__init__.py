from urllib.parse import urlparse
from collections import defaultdict

import dvc.output as output
from dvc.dependency.gs import GSDependency
from dvc.dependency.hdfs import HDFSDependency
from dvc.dependency.http import HTTPDependency
from dvc.dependency.https import HTTPSDependency
from dvc.dependency.local import LocalDependency
from dvc.dependency.s3 import S3Dependency
from dvc.dependency.ssh import SSHDependency
from dvc.dependency.param import ParamsDependency
from dvc.output.base import BaseOutput
from dvc.remote import Remote
from dvc.scheme import Schemes
from .repo import RepoDependency


DEPS = [
    GSDependency,
    HDFSDependency,
    HTTPDependency,
    HTTPSDependency,
    S3Dependency,
    SSHDependency,
    # NOTE: LocalDependency is the default choice
]

DEP_MAP = {
    Schemes.LOCAL: LocalDependency,
    Schemes.SSH: SSHDependency,
    Schemes.S3: S3Dependency,
    Schemes.GS: GSDependency,
    Schemes.HDFS: HDFSDependency,
    Schemes.HTTP: HTTPDependency,
    Schemes.HTTPS: HTTPSDependency,
}


# NOTE: schema for dependencies is basically the same as for outputs, but
# without output-specific entries like 'cache' (whether or not output is
# cached, see -o and -O flags for `dvc run`) and 'metric' (whether or not
# output is a metric file and how to parse it, see `-M` flag for `dvc run`).
SCHEMA = output.SCHEMA.copy()
del SCHEMA[BaseOutput.PARAM_CACHE]
del SCHEMA[BaseOutput.PARAM_METRIC]
SCHEMA.update(RepoDependency.REPO_SCHEMA)
SCHEMA.update(ParamsDependency.PARAM_SCHEMA)


def _get(stage, p, info):
    parsed = urlparse(p) if p else None
    if parsed and parsed.scheme == "remote":
        remote = Remote(stage.repo, name=parsed.netloc)
        return DEP_MAP[remote.scheme](stage, p, info, remote=remote)

    if info and info.get(RepoDependency.PARAM_REPO):
        repo = info.pop(RepoDependency.PARAM_REPO)
        return RepoDependency(repo, stage, p, info)

    if info and info.get(ParamsDependency.PARAM_PARAMS):
        params = info.pop(ParamsDependency.PARAM_PARAMS)
        return ParamsDependency(stage, p, params)

    for d in DEPS:
        if d.supported(p):
            return d(stage, p, info)
    return LocalDependency(stage, p, info)


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(BaseOutput.PARAM_PATH, None)
        ret.append(_get(stage, p, d))
    return ret


def loads_from(stage, s_list, erepo=None):
    ret = []
    for s in s_list:
        info = {RepoDependency.PARAM_REPO: erepo} if erepo else {}
        ret.append(_get(stage, s, info))
    return ret


def _parse_params(path_params):
    path, _, params_str = path_params.rpartition(":")
    params = params_str.split(",")
    return path, params


def loads_params(stage, s_list):
    # Creates an object for each unique file that is referenced in the list
    params_by_path = defaultdict(list)
    for s in s_list:
        path, params = _parse_params(s)
        params_by_path[path].extend(params)

    d_list = []
    for path, params in params_by_path.items():
        d_list.append(
            {
                BaseOutput.PARAM_PATH: path,
                ParamsDependency.PARAM_PARAMS: params,
            }
        )

    return loadd_from(stage, d_list)
