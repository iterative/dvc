try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.dependency.base import DependencyBase
from dvc.cloud.aws import DataCloudAWS
from dvc.remote.s3 import RemoteS3
from dvc.config import Config


class DependencyS3(DependencyBase):
    REGEX = DataCloudAWS.REGEX

    def __init__(self, stage, path, info=None):
        super(DependencyS3, self).__init__(stage, path)
        self.info = info
        self.remote = RemoteS3(stage.project, {})
        self.path_info = {'scheme': 's3',
                          'bucket': urlparse(path).netloc,
                          'key': urlparse(path).path.lstrip('/')}

    def changed(self):
        return self.info != self.remote.save_info(self.path_info)

    def save(self):
        self.info = self.remote.save_info(self.path_info)

    def dumpd(self):
        ret = self.info
        ret[self.PARAM_PATH] = self.path
        return ret
