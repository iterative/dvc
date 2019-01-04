try:
    from urlparse import urlparse, urljoin
except ImportError:
    from urllib.parse import urlparse, urljoin

from dvc.dependency.base import DependencyBase
from dvc.remote.http import RemoteHTTP


class DependencyHTTP(DependencyBase):
    REGEX = RemoteHTTP.REGEX

    def __init__(self, stage, path, info=None, remote=None):
        super(DependencyHTTP, self).__init__(stage, path, info=info)

        self.remote = remote or RemoteHTTP(stage.project, {})

        if path.startswith('remote'):
            path = urljoin(self.remote.cache_dir, urlparse(path).path)

        self.path_info = {
            'scheme': urlparse(path).scheme,
            'path': path,
        }

    def save(self):
        self.info = self.remote.save_info(self.path_info)

    def changed(self):
        if not self.exists:
            return True

        return self.info != self.remote.save_info(self.path_info)
