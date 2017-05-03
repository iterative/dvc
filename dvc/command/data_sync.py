import base64
import hashlib
import os

import fasteners
from boto.s3.connection import S3Connection
from google.cloud import storage as gc

from dvc.command.base import CmdBase
from dvc.command.init import CmdInit
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.runtime import Runtime
from dvc.system import System


class DataSyncError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Data sync error: {}'.format(msg))


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)


def percent_cb(complete, total):
    Logger.debug('{} transferred out of {}'.format(sizeof_fmt(complete), sizeof_fmt(total)))


def file_md5(fname):
    """ get the (md5 hexdigest, md5 digest) of a file """
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1000), b""):
            hash_md5.update(chunk)
    return (hash_md5.hexdigest(), hash_md5.digest())


class CmdDataSync(CmdBase):
    def __init__(self, settings):
        super(CmdDataSync, self).__init__(settings)

    def define_args(self, parser):
        parser.add_argument('targets',
                            metavar='',
                            help='File or directory to sync.',
                            nargs='*')

    def run(self):
        if self.is_locker:
            lock = fasteners.InterProcessLock(self.git.lock_file)
            gotten = lock.acquire(timeout=5)
            if not gotten:
                Logger.info('Cannot perform the cmd since DVC is busy and locked. Please retry the cmd later.')
                return 1

        good = self.config.sanity_check()

        if not good[0]:
            Logger.error('config \'%s\' is not correctly setup.  Please fix:' % Runtime.CONFIG)
            for e in good[1]:
                Logger.error('    ' + e)
            return 1

        try:
            for target in self.parsed_args.targets:
                data_item = self.settings.path_factory.data_item(target)
                if System.islink(target):
                    self.sync_symlink(data_item)
                elif os.path.isdir(target):
                    self.sync_dir(target)
                else:
                    raise DataSyncError('File "{}" does not exit'.format(target))
        finally:
            if self.is_locker:
                lock.release()

    def sync_dir(self, dir):
        for file in os.listdir(dir):
            try:
                fname = None
                fname = os.path.join(dir, file)
                if os.path.isdir(fname):
                    self.sync_dir(fname)
                elif System.islink(fname):
                    self.sync_symlink(self.settings.path_factory.data_item(fname))
                else:
                    raise DvcException('Unsupported file type "{}"'.format(fname))
            except DataSyncError as ex:
                Logger.debug(ex)
            except Exception as ex:
                Logger.error('Cannot sync file {}: {}'.format(fname, ex))
        pass

    def sync_symlink(self, data_item):
        if os.path.isfile(data_item.resolved_cache.dvc):
            self.sync_to_cloud(data_item)
        else:
            self.sync_from_cloud(data_item)

    def _get_bucket_aws(self):
        """ get a bucket object, aws """
        conn = S3Connection(self.config.aws_access_key_id, self.config.aws_secret_access_key, host=self.config.aws_region_host)
        bucket_name = self.config.storage_bucket
        bucket = conn.lookup(bucket_name)
        if bucket is None:
            raise DataSyncError('Storage path is not setup correctly')
        return bucket

    def _sync_from_cloud_aws(self, item):
        """ sync from cloud, aws version """

        bucket = self._get_bucket_aws()

        key_name = self.cache_file_key(item.resolved_cache.dvc)
        key = bucket.get_key(key_name)
        if not key:
            raise DataSyncError('File "{}" does not exist in the cloud'.format(key_name))

        Logger.info('Downloading cache file from S3 "{}/{}"'.format(bucket.name, key_name))
        key.get_contents_to_filename(item.resolved_cache.relative, cb=percent_cb)
        Logger.info('Downloading completed')

    def _sync_to_cloud_aws(self, data_item):
        """ sync_to_cloud, aws version """

        aws_key = self.cache_file_key(data_item.resolved_cache.dvc)
        bucket = self._get_bucket_aws()
        key = bucket.get_key(aws_key)
        if key:
            Logger.debug('File already uploaded to the cloud. Checksum validation...')

            md5_cloud = key.etag[1:-1]
            md5_local = file_md5(data_item.resolved_cache.dvc)[0]
            if md5_cloud == md5_local:
                Logger.debug('File checksum matches. No uploading is needed.')
                return

            Logger.debug('Checksum miss-match. Re-uploading is required.')

        Logger.info('Uploading cache file "{}" to S3 "{}"'.format(data_item.resolved_cache.relative, aws_key))
        key = bucket.new_key(aws_key)
        key.set_contents_from_filename(data_item.resolved_cache.relative, cb=percent_cb)
        Logger.info('Uploading completed')


    def _get_bucket_gc(self):
        """ get a bucket object, gc """
        client = gc.Client(project=self.config.gc_project_name)
        bucket = client.bucket(self.config.storage_bucket)
        if not bucket.exists():
            raise DataSyncError('sync up: google cloud bucket {} doesn\'t exist'.format(self.config.storage_bucket))
        return bucket

    def _sync_from_cloud_gcp(self, item):
        """ sync from cloud, gcp version
        Params:
            TODO
        """

        bucket = self._get_bucket_gc()
        key = self.cache_file_key(item.resolved_cache.dvc)

        blob = bucket.get_blob(key)
        if not blob:
            raise DataSyncError('File "{}" does not exist in the cloud'.format(key))

        Logger.info('Downloading cache file from gc "{}/{}"'.format(bucket.name, key))

        blob.download_to_filename(item.resolved_cache.dvc)
        Logger.info('Downloading completed')

    def _sync_to_cloud_gcp(self, data_item):
        """ sync_to_cloud, gcp version """

        bucket = self._get_bucket_gc()
        blob_name = self.cache_file_key(data_item.resolved_cache.dvc)

        blob = bucket.get_blob(blob_name)
        if blob is not None and blob.exists():
            b64_encoded_md5 = base64.b64encode(file_md5(data_item.resolved_cache.dvc)[1])

            if blob.md5_hash == b64_encoded_md5:
                Logger.debug('checksum %s matches.  Skipping upload' % data_item.cache.relative)
                return
            Logger.debug('checksum %s mismatch.  re-uploading' % data_item.cache.relative)

        Logger.info('uploading cache file "{} to gc "{}"'.format(data_item.cache.relative, blob_name))

        blob = bucket.blob(blob_name)
        blob.upload_from_filename(data_item.resolved_cache.relative)
        Logger.info('uploading %s completed' % data_item.resolved_cache.relative)

    def sync_from_cloud(self, item):
        cloud = self.settings.config.cloud
        assert cloud in ['aws', 'gcp'], 'unknown cloud %s' % cloud
        if cloud == 'aws':
            return self._sync_from_cloud_aws(item)
        elif cloud == 'gcp':
            return self._sync_from_cloud_gcp(item)

    def sync_to_cloud(self, data_item):
        cloud = self.settings.config.cloud
        assert cloud in ['aws', 'gcp'], 'unknown cloud %s' % cloud
        if cloud == 'aws':
            return self._sync_to_cloud_aws(data_item)
        elif cloud == 'gcp':
            return self._sync_to_cloud_gcp(data_item)


if __name__ == '__main__':
    Runtime.run(CmdDataSync)
