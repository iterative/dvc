import os
import math
import threading

import boto3

from dvc.config import Config
from dvc.logger import Logger
from dvc.progress import progress
from dvc.cloud.base import DataCloudError, DataCloudBase


def sizeof_fmt(num, suffix='B'):
    """ Convert number of bytes to human-readable string """
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)


def percent_cb(name, complete, total):
    """ Callback for updating target progress """
    Logger.debug('{}: {} transferred out of {}'.format(name,
                                                       sizeof_fmt(complete),
                                                       sizeof_fmt(total)))
    progress.update_target(name, complete, total)


def create_cb(name):
    """ Create callback function for multipart object """
    return (lambda cur, tot: percent_cb(name, cur, tot))


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


class DataCloudAWS(DataCloudBase):
    """ DataCloud class for Amazon Web Services """
    REGEX = r'^s3://(?P<path>.*)$'

    @property
    def profile(self):
        return self._cloud_settings.cloud_config.get(Config.SECTION_AWS_PROFILE, None)

    def connect(self):
        session = boto3.Session(profile_name=self.profile)
        self.s3 = session.resource('s3')
        bucket = self.s3.Bucket(self.storage_bucket)
        if bucket is None:
            raise DataCloudError('Storage path {} is not setup correctly'.format(self.storage_bucket))

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
        name = os.path.relpath(fname, self._cloud_settings.cache.cache_dir)

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
            self.s3.Object(self.storage_bucket, key_name).get()
            return AWSKey(self.storage_bucket, key_name)
        except Exception:
            return None

    def _new_key(self, path):
        key_name = self.cache_file_key(path)
        return AWSKey(self.storage_bucket, key_name)

    def _push_key(self, key, path):
        """ push, aws version """
        name = os.path.relpath(path, self._cloud_settings.cache.cache_dir)
        cb = self.create_cb_push(name, path)
        try:
            self.s3.Object(key.bucket, key.name).upload_file(path, Callback=cb)
        except Exception as exc:
            Logger.error('Failed to upload "{}": {}'.format(path, exc))
            return None

        progress.finish_target(name)

        return path
