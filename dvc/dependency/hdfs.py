import posixpath

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.dependency.base import DependencyBase
from dvc.remote.hdfs import RemoteHDFS


class DependencyHDFS(DependencyBase):
    REGEX = RemoteHDFS.REGEX

    def __init__(self, stage, path, info=None, remote=None):
        super(DependencyHDFS, self).__init__(stage, path)
        self.info = info
        self.remote = remote if remote else RemoteHDFS(stage.project, {})
        if remote:
            path = posixpath.join(remote.url, urlparse(path).path.lstrip('/'))
        user = remote.user if remote else self.group('user')
        self.path_info = {'scheme': 'hdfs',
                          'user': user,
                          'url': path}

    def changed(self):
        return self.info != self.remote.save_info(self.path_info)

    def save(self):
        self.info = self.remote.save_info(self.path_info)

    def dumpd(self):
        ret = self.info
        ret[self.PARAM_PATH] = self.path
        return ret
