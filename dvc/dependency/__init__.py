from urllib.parse import urlparse

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


def _get_by_path(stage, path, info):
    parsed = urlparse(path)

    if parsed.scheme == "remote":
        remote = Remote(stage.repo, name=parsed.netloc)
        return DEP_MAP[remote.scheme](stage, path, info, remote=remote)

    if info and info.get(DependencyREPO.PARAM_REPO):
        repo = info.pop(DependencyREPO.PARAM_REPO)
        return DependencyREPO(repo, stage, path, info)

    for d in DEPS:
        if d.supported(path):
            return d(stage, path, info)
    return DependencyLOCAL(stage, path, info)


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(OutputBase.PARAM_PATH)
        ret.append(_get_by_path(stage, p, d))
    return ret


def loads_from(stage, s_list, erepo=None):
    ret = []
    for s in s_list:
        info = {DependencyREPO.PARAM_REPO: erepo} if erepo else {}
        dep_obj = _get_by_path(stage, s, info)
        ret.append(dep_obj)
    return ret


def loads_params(stage, s_list):  # TODO: Make support for `eropo=` as well ?
    return DependencyPARAMS.from_list(stage, s_list)
