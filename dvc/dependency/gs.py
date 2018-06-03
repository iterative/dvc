try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.cloud.gcp import DataCloudGCP
from dvc.dependency.s3 import DependencyS3
from dvc.remote.gs import RemoteGS
from dvc.config import Config


class DependencyGS(DependencyS3):
    REGEX = DataCloudGCP.REGEX

    def __init__(self, stage, path, info=None):
        super(DependencyGS, self).__init__(stage, path)
        self.info = info
        self.remote = RemoteGS(stage.project, {Config.SECTION_REMOTE_URL: '/'})
        self.path_info = {'scheme': 'gs',
                          'bucket': urlparse(path).netloc,
                          'key': urlparse(path).path.lstrip('/')}
