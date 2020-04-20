from .webdav import RemoteWEBDAV
from dvc.scheme import Schemes


class RemoteWEBDAVS(RemoteWEBDAV):
    scheme = Schemes.WEBDAVS

    def gc(self):
        raise NotImplementedError

    def list_cache_paths(self, prefix=None, progress_callback=None):
        raise NotImplementedError

    def walk_files(self, path_info):
        raise NotImplementedError
