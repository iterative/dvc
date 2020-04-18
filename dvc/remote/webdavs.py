from .webdav import RemoteWEBDAV
from dvc.scheme import Schemes


class RemoteWEBDAVS(RemoteWEBDAV):
    scheme = Schemes.WEBDAVS
