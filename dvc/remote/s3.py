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

    def upload(self, path, path_info):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        self.s3.Object(path_info['bucket'], path_info['key']).upload_file(path)


    def download(self, path_info, path):
        if path_info['scheme'] != 's3':
            raise NotImplementedError

        self.s3.Object(path_info['bucket'], path_info['key']).download_file(path)

    # Old code starting from here
    def create_cb_pull(self, name, key):
        total = self.s3.Object(bucket_name=key.bucket, key=key.name).content_length
        return Callback(name, total)

    def create_cb_push(self, name, fname):
        total = os.path.getsize(fname)
        return Callback(name, total)

    def _pull_key(self, key, fname, no_progress_bar=False):
        Logger.debug("Pulling key '{}' from bucket '{}' to file '{}'".format(key.name,
                                                                             key.bucket,
                                                                             fname))
        self._makedirs(fname)

        tmp_file = self.tmp_file(fname)
        name = self.cache_key_name(fname)

        if self._cmp_checksum(key, fname):
            Logger.debug('File "{}" matches with "{}".'.format(fname, key.name))
            return fname

        Logger.debug('Downloading cache file from S3 "{}/{}" to "{}"'.format(key.bucket,
                                                                             key.name,
                                                                             fname))

        if no_progress_bar:
            cb = None
        else:
            cb = self.create_cb_pull(name, key)


        try:
            self.s3.Object(key.bucket, key.name).download_file(tmp_file, Callback=cb)
        except Exception as exc:
            Logger.error('Failed to download "{}": {}'.format(key.name, exc))
            return None

        os.rename(tmp_file, fname)

        if not no_progress_bar:
            progress.finish_target(name)

        Logger.debug('Downloading completed')

        return fname

    def _get_key(self, path):
        key_name = self.cache_file_key(path)
        try:
            self.s3.Object(self.bucket, key_name).get()
            return AWSKey(self.bucket, key_name)
        except Exception:
            return None

    def _new_key(self, path):
        key_name = self.cache_file_key(path)
        return AWSKey(self.bucket, key_name)

    def _push_key(self, key, path):
        """ push, aws version """
        name = self.cache_key_name(path)
        cb = self.create_cb_push(name, path)
        try:
            self.s3.Object(key.bucket, key.name).upload_file(path, Callback=cb)
        except Exception as exc:
            Logger.error('Failed to upload "{}": {}'.format(path, exc))
            return None

        progress.finish_target(name)

        return path
