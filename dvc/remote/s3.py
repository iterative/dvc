import os
import math
import boto3
import threading
import posixpath

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.logger import Logger
from dvc.progress import progress
from dvc.config import Config
from dvc.remote.base import RemoteBase


class Callback(object):
    def __init__(self, name, total):
        self.name = name
        self.total = total
        self.current = 0
        self.lock = threading.Lock()

    def __call__(self, byts):
        with self.lock:
            self.current += byts
            progress.update_target(self.name, self.current, self.total)


class AWSKey(object):
    def __init__(self, bucket, name):
        self.name = name
        self.bucket = bucket


class RemoteS3(RemoteBase):
    scheme = 's3'
    REGEX = r'^s3://(?P<path>.*)$'
    PARAM_ETAG = 'etag'

    def __init__(self, project, config):
        self.project = project
        storagepath = 's3://' + config.get(Config.SECTION_AWS_STORAGEPATH, '').lstrip('/')
        self.url = config.get(Config.SECTION_REMOTE_URL, storagepath)
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

    def remove(self, path_info):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        try:
            obj = self.s3.Object(path_info['bucket'], path_info['key']).get()
            obj.delete()
        except Exception:
            pass

    def _get_path_info(self, path):
        key = self.cache_file_key(path)
        try:
            self.s3.Object(self.bucket, key).get()
            return {'scheme': self.scheme,
                    'bucket': self.bucket,
                    'key': key}
        except Exception:
            return None

    def _new_path_info(self, path):
        key = self.cache_file_key(path)
        return {'scheme': self.scheme,
                'bucket': self.bucket,
                'key': key}

    def upload(self, path, path_info, name=None):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        Logger.debug("Uploading '{}' to '{}/{}'".format(path,
                                                        path_info['bucket'],
                                                        path_info['key']))

        if not name:
            name = os.path.basename(path)

        total = os.path.getsize(path)
        cb = Callback(name, total)

        try:
            self.s3.Object(path_info['bucket'], path_info['key']).upload_file(path, Callback=cb)
        except Exception as exc:
            Logger.error("Failed to upload '{}'".format(path), exc)
            return None

        progress.finish_target(name)

        return path

    def download(self, path_info, fname, no_progress_bar=False, name=None):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        Logger.debug("Downloading '{}/{}' to '{}'".format(path_info['bucket'],
                                                          path_info['key'],
                                                          fname))

        tmp_file = self.tmp_file(fname)
        if not name:
            name = os.path.basename(fname)

        if no_progress_bar:
            cb = None
        else:
            total = self.s3.Object(bucket_name=path_info['bucket'],
                                   key=path_info['key']).content_length
            cb = Callback(name, total)

        self._makedirs(fname)

        try:
            self.s3.Object(path_info['bucket'], path_info['key']).download_file(tmp_file, Callback=cb)
        except Exception as exc:
            Logger.error("Failed to download '{}/{}'".format(path_info['bucket'],
                                                             path_info['key']), exc)
            return None

        os.rename(tmp_file, fname)

        if not no_progress_bar:
            progress.finish_target(name)

        return fname
