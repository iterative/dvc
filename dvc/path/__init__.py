from dvc.scheme import Schemes

from dvc.path.azure import PathAZURE
from dvc.path.gs import PathGS
from dvc.path.hdfs import PathHDFS
from dvc.path.http import PathHTTP
from dvc.path.https import PathHTTPS
from dvc.path.local import PathLOCAL
from dvc.path.oss import PathOSS
from dvc.path.s3 import PathS3
from dvc.path.ssh import PathSSH


PATH_MAP = {
    Schemes.SSH: PathSSH,
    Schemes.HDFS: PathHDFS,
    Schemes.S3: PathS3,
    Schemes.AZURE: PathAZURE,
    Schemes.HTTP: PathHTTP,
    Schemes.HTTPS: PathHTTPS,
    Schemes.GS: PathGS,
    Schemes.LOCAL: PathLOCAL,
    Schemes.OSS: PathOSS,
}


def Path(scheme, *args, **kwargs):
    cls = PATH_MAP[scheme]
    return cls(*args, **kwargs)
