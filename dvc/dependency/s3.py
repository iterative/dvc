import boto3

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.dependency.base import DependencyBase
from dvc.cloud.aws import DataCloudAWS


class DependencyS3(DependencyBase):
    REGEX = DataCloudAWS.REGEX

    PARAM_ETAG = 'etag'

    def __init__(self, stage, path, etag=None):
        super(DependencyS3, self).__init__(stage, path)
        self.etag = etag

    @property
    def bucket(self):
        return urlparse(self.path).netloc

    @property
    def key(self):
        return urlparse(self.path).path.lstrip('/')

    @property
    def s3(self):
        session = boto3.Session()
        return session.resource('s3')

    def get_etag(self):
        try:
            obj = self.s3.Object(self.bucket, self.key).get()
        except Exception:
            return None

        return obj['ETag'].strip('"')

    def changed(self):
        return self.etag != self.get_etag()

    def save(self):
        self.etag = self.get_etag()

    def dumpd(self):
        return {self.PARAM_PATH: self.path,
                self.PARAM_ETAG: self.etag}
