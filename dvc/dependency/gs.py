from google.cloud import storage as gc

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.dependency.s3 import DependencyS3
from dvc.cloud.gcp import DataCloudGCP


class DependencyGS(DependencyS3):
    REGEX = DataCloudGCP.REGEX

    def get_etag(self):
        o = urlparse(self.path)
        bucket_name = o.netloc
        key = o.path.lstrip('/')

        client = gc.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.get_blob(key)
        return blob.etag
