from __future__ import unicode_literals

import schema

from dvc.scheme import Schemes

import dvc.output as output
from dvc.output.base import OutputBase
from dvc.dependency.s3 import DependencyS3
from dvc.dependency.gs import DependencyGS
from dvc.dependency.local import DependencyLOCAL
from dvc.dependency.hdfs import DependencyHDFS
from dvc.dependency.ssh import DependencySSH
from dvc.dependency.http import DependencyHTTP
from dvc.dependency.https import DependencyHTTPS
from .pkg import DependencyPKG

from dvc.remote import Remote
from dvc.pkg import Pkg


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
del SCHEMA[schema.Optional(OutputBase.PARAM_CACHE)]
del SCHEMA[schema.Optional(OutputBase.PARAM_METRIC)]
SCHEMA[schema.Optional(DependencyPKG.PARAM_PKG)] = Pkg.SCHEMA


def _get(stage, p, info):
    from dvc.utils.compat import urlparse

    parsed = urlparse(p)

    if parsed.scheme == "remote":
        remote = Remote(stage.repo, name=parsed.netloc)
        return DEP_MAP[remote.scheme](stage, p, info, remote=remote)

    if info and info.get(DependencyPKG.PARAM_PKG):
        pkg = info.pop(DependencyPKG.PARAM_PKG)
        return DependencyPKG(pkg, stage, p, info)

    for d in DEPS:
        if d.supported(p):
            return d(stage, p, info)
    return DependencyLOCAL(stage, p, info)


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(OutputBase.PARAM_PATH)
        ret.append(_get(stage, p, d))
    return ret


def loads_from(stage, s_list, pkg=None):
    ret = []
    for s in s_list:
        info = {DependencyPKG.PARAM_PKG: pkg} if pkg else {}
        ret.append(_get(stage, s, info))
    return ret
