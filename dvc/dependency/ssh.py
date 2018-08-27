import posixpath

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.dependency.base import DependencyBase
from dvc.remote.ssh import RemoteSSH


class DependencySSH(DependencyBase):
    REGEX = RemoteSSH.REGEX

    def __init__(self, stage, path, info=None, remote=None):
        super(DependencySSH, self).__init__(stage, path)
        self.info = info
        self.remote = remote if remote else RemoteSSH(stage.project, {})

        host = remote.host if remote else self.group('host')
        port = remote.port if remote else RemoteSSH.DEFAULT_PORT
        user = remote.user if remote else self.group('user')
        if remote:
            path = posixpath.join(remote.prefix,
                                  urlparse(path).path.lstrip('/'))
        else:
            path = self.match(self.path).group('path')

        self.path_info = {'scheme': 'ssh',
                          'host': host,
                          'port': port,
                          'user': user,
                          'path': path}

    def changed(self):
        return self.info != self.remote.save_info(self.path_info)

    def save(self):
        self.info = self.remote.save_info(self.path_info)

    def dumpd(self):
        ret = self.info
        ret[self.PARAM_PATH] = self.path
        return ret
