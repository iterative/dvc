import boto3
import posixpath

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.remote.base import RemoteBase


class RemoteS3(RemoteBase):
    REGEX = r'^s3://(?P<path>.*)$'
    PARAM_ETAG = 'etag'

    def __init__(self, project, config):
        #FIXME
        from dvc.config import Config


        self.project = project
        self.url = config[Config.SECTION_REMOTE_URL]
        self.region = config.get(Config.SECTION_AWS_REGION, None)
        self.profile = config.get(Config.SECTION_AWS_PROFILE, None)
        self.credentialpath = config.get(Config.SECTION_AWS_CREDENTIALPATH, None)

    @property
    def bucket(self):
        return urlparse(self.url).netloc

    @property
    def prefix(self):
        return urlparse(self.url).path.lstrip('/')

    @property
    def s3(self):
        return boto3.resource('s3')

    def get_etag(self, bucket, key):
        try:
            obj = self.s3.Object(bucket, key).get()
        except Exception:
            return None

        return obj['ETag'].strip('"')

    def save_info(self, path_info):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        return {self.PARAM_ETAG: self.get_etag(path_info['bucket'], path_info['key'])}

    def save(self, path_info):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        etag = self.get_etag(path_info['bucket'], path_info['key'])
        dest_key = posixpath.join(self.prefix, etag[0:2], etag[2:])

        source = {'Bucket': path_info['bucket'],
                  'Key': path_info['key']}
        self.s3.Bucket(self.bucket).copy(source, dest_key)

        return {self.PARAM_ETAG: etag}

    def checkout(self, path_info, checksum_info):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        etag = checksum_info.get(self.PARAM_ETAG, None)
        if not etag:
            return

        key = posixpath.join(self.prefix, etag[0:2], etag[2:])
        source = {'Bucket': self.bucket,
                  'Key': key}

        self.s3.Bucket(path_info['bucket']).copy(source, path_info['key'])
