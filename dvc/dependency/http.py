from __future__ import unicode_literals

from dvc.path.utils import PathInfo
from dvc.utils.compat import urlparse, urljoin
from dvc.output.base import OutputBase
from dvc.remote.http import RemoteHTTP
from dvc.dependency.base import DependencyBase


class DependencyHTTP(DependencyBase, OutputBase):
    REMOTE = RemoteHTTP

    def __init__(self, stage, path, info=None, remote=None):
        super(DependencyHTTP, self).__init__(
            stage, path, info=info, remote=remote
        )
        if path.startswith("remote"):
            path = urljoin(self.remote.cache_dir, urlparse(path).path)

        self.path_info = PathInfo(self.scheme, url=self.url, path=path)
