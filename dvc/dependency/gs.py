from google.cloud import storage as gc

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.dependency.s3 import DependencyS3
from dvc.cloud.gcp import DataCloudGCP


class DependencyGS(DependencyS3):
    REGEX = DataCloudGCP.REGEX

    @property
    def client(self):
        return gc.Client()

    def get_etag(self):
        bucket = self.client.bucket(self.bucket)
        blob = bucket.get_blob(self.key)
        if not blob:
            return None

        return blob.etag
