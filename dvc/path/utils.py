from dvc.path import Schemes
from dvc.path.azure import AzurePathInfo
from dvc.path.gs import GSPathInfo
from dvc.path.hdfs import HDFSPathInfo
from dvc.path.http import HTTPPathInfo
from dvc.path.local import LocalPathInfo
from dvc.path.oss import OSSPathInfo
from dvc.path.s3 import S3PathInfo
from dvc.path.ssh import SSHPathInfo

PATH_MAP = {
    Schemes.SSH: SSHPathInfo,
    Schemes.HDFS: HDFSPathInfo,
    Schemes.S3: S3PathInfo,
    Schemes.AZURE: AzurePathInfo,
    Schemes.HTTP: HTTPPathInfo,
    Schemes.GS: GSPathInfo,
    Schemes.LOCAL: LocalPathInfo,
    Schemes.OSS: OSSPathInfo,
}


def PathInfo(scheme, *args, **kwargs):
    cls = PATH_MAP[scheme]
    return cls(*args, **kwargs)
