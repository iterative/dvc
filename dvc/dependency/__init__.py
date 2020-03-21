from urllib.parse import urlparse
from collections import defaultdict

import dvc.output as output
from dvc.dependency.gs import DependencyGS
from dvc.dependency.hdfs import DependencyHDFS
from dvc.dependency.http import DependencyHTTP
from dvc.dependency.https import DependencyHTTPS
from dvc.dependency.local import DependencyLOCAL
from dvc.dependency.s3 import DependencyS3
from dvc.dependency.ssh import DependencySSH
from dvc.dependency.param import DependencyPARAMS
from dvc.output.base import OutputBase
from dvc.remote import Remote
from dvc.scheme import Schemes
from .repo import DependencyREPO


DEPS = [
    DependencyGS,
    DependencyHDFS,
    DependencyHTTP,
    DependencyHTTPS,
    DependencyS3,
    DependencySSH,
    # NOTE: DependencyLOCAL is the default choice
]

DEP_MAP = {
    Schemes.LOCAL: DependencyLOCAL,
    Schemes.SSH: DependencySSH,
    Schemes.S3: DependencyS3,
    Schemes.GS: DependencyGS,
    Schemes.HDFS: DependencyHDFS,
    Schemes.HTTP: DependencyHTTP,
    Schemes.HTTPS: DependencyHTTPS,
}


# NOTE: schema for dependencies is basically the same as for outputs, but
# without output-specific entries like 'cache' (whether or not output is
# cached, see -o and -O flags for `dvc run`) and 'metric' (whether or not
# output is a metric file and how to parse it, see `-M` flag for `dvc run`).
SCHEMA = output.SCHEMA.copy()
del SCHEMA[OutputBase.PARAM_CACHE]
del SCHEMA[OutputBase.PARAM_METRIC]
SCHEMA.update(DependencyREPO.REPO_SCHEMA)
SCHEMA.update(DependencyPARAMS.PARAM_SCHEMA)


def _get(stage, p, info):
    parsed = urlparse(p) if p else None
    if parsed and parsed.scheme == "remote":
        remote = Remote(stage.repo, name=parsed.netloc)
        return DEP_MAP[remote.scheme](stage, p, info, remote=remote)

    if info and info.get(DependencyREPO.PARAM_REPO):
        repo = info.pop(DependencyREPO.PARAM_REPO)
        return DependencyREPO(repo, stage, p, info)

    if info and info.get(DependencyPARAMS.PARAM_PARAMS):
        params = info.pop(DependencyPARAMS.PARAM_PARAMS)
        return DependencyPARAMS(stage, p, params)

    for d in DEPS:
        if d.supported(p):
            return d(stage, p, info)
    return DependencyLOCAL(stage, p, info)


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(OutputBase.PARAM_PATH, None)
        ret.append(_get(stage, p, d))
    return ret


def loads_from(stage, s_list, erepo=None):
    ret = []
    for s in s_list:
        info = {DependencyREPO.PARAM_REPO: erepo} if erepo else {}
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
                OutputBase.PARAM_PATH: path,
                DependencyPARAMS.PARAM_PARAMS: params,
            }
        )

    return loadd_from(stage, d_list)
