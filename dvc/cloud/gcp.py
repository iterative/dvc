import base64
import os

from google.cloud import storage as gc

from dvc.logger import Logger
from dvc.config import ConfigError
from dvc.cloud.base import DataCloudError, DataCloudBase
from dvc.utils import file_md5
from dvc.progress import progress


class DataCloudGCP(DataCloudBase):
    """ DataCloud class for Google Cloud Platform """
    @property
    def gc_project_name(self):
        """
        Get project name from config.
        """
        return self._cloud_settings.cloud_config.get('ProjectName', None)

    def sanity_check(self):
        project = self.gc_project_name
        if project is None or len(project) < 1:
            raise ConfigError('can\'t read google cloud project name. '
                              'Please set ProjectName in section GC.')

    def _get_bucket_gc(self, storage_bucket):
        """ get a bucket object, gc """
        client = gc.Client(project=self.gc_project_name)
        bucket = client.bucket(storage_bucket)
        if not bucket.exists():
            raise DataCloudError('sync up: google cloud bucket {} '
                                 'doesn\'t exist'.format(self.storage_bucket))
        return bucket

    @staticmethod
    def _cmp_checksum(blob, fname):
        """
        Verify local and remote checksums.
        """
        md5 = file_md5(fname)[1]
        b64_encoded_md5 = base64.b64encode(md5).decode() if md5 else None

        if blob.md5_hash == b64_encoded_md5:
            return True

        return False

    def _import(self, bucket_name, key, fname):

        bucket = self._get_bucket_gc(bucket_name)

        name = os.path.basename(fname)
        tmp_file = self.tmp_file(fname)

        blob = bucket.get_blob(key)
        if not blob:
            Logger.error('File "{}" does not exist in the cloud'.format(key))
            return None

        if self._cmp_checksum(blob, fname):
            Logger.debug('File "{}" matches with "{}".'.format(fname, key))
            return fname

        Logger.debug('Downloading cache file from gc "{}/{}"'.format(bucket.name, key))

        # percent_cb is not available for download_to_filename, so
        # lets at least update progress at keypoints(start, finish)
        progress.update_target(name, 0, None)

        try:
            blob.download_to_filename(tmp_file)
        except Exception as exc:
            Logger.error('Failed to download "{}": {}'.format(key, exc))
            return None

        os.rename(tmp_file, fname)

        progress.finish_target(name)

        Logger.debug('Downloading completed')

        return fname

    def push(self, path):
        """ push, gcp version """

        bucket = self._get_bucket_gc(self.storage_bucket)
        blob_name = self.cache_file_key(path)
        name = os.path.basename(path)

        blob = bucket.get_blob(blob_name)
        if blob is not None and blob.exists():
            if self._cmp_checksum(blob, path):
                Logger.debug('checksum %s matches.  Skipping upload' % path)
                return path
            Logger.debug('checksum %s mismatch.  re-uploading' % path)

        # same as in _import
        progress.update_target(name, 0, None)

        blob = bucket.blob(blob_name)
        blob.upload_from_filename(path)

        progress.finish_target(name)
        Logger.debug('uploading %s completed' % path)

        return path

    def _status(self, path):
        """ status, gcp version """

        bucket = self._get_bucket_gc(self.storage_bucket)
        blob_name = self.cache_file_key(path)
        blob = bucket.get_blob(blob_name)

        remote_exists = blob is not None and blob.exists()
        local_exists = os.path.exists(path)
        diff = None
        if remote_exists and local_exists:
            diff = self._cmp_checksum(blob, path)

        return (local_exists, remote_exists, diff)

    def remove(self, item):
        bucket = self._get_bucket_gc(self.storage_bucket)
        blob_name = self.cache_file_key(path)
        blob = bucket.blob(blob_name)
        blob.delete()
