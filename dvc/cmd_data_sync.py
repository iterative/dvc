import hashlib
import os

from boto.s3.connection import S3Connection

from dvc.cmd_base import CmdBase
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.utils import run


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
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1000), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class CmdDataSync(CmdBase):
    def __init__(self):
        CmdBase.__init__(self)

        conn = S3Connection(self.config.aws_access_key_id, self.config.aws_secret_access_key)

        bucket_name = self.config.aws_storage_bucket
        self._bucket = conn.lookup(bucket_name)
        if not self._bucket:
            self._bucket = conn.create_bucket(bucket_name)
            Logger.printing('S3 bucket "{}" was created'.format(bucket_name))
        pass

    def define_args(self, parser):
        self.add_string_arg(parser, 'target', 'Target to sync - file or directory')
        pass

    def run(self):
        if os.path.islink(self.args.target):
            data_path = self.path_factory.existing_data_path(self.args.target)
            return self.sync_symlink(data_path)

        if os.path.isdir(self.args.target):
            return self.sync_dir(self.args.target)

        raise DataSyncError('File "{}" does not exit'.format(target))

    def sync_dir(self, dir):
        for f in os.listdir(dir):
            fname = os.path.join(dir, f)
            if os.path.isdir(fname):
                self.sync_dir(fname)
            elif os.path.islink(fname):
                self.sync_symlink(self.path_factory.existing_data_path(fname))
            else:
                raise DataSyncError('Unsupported file type "{}"'.format(fname))
        pass

    def sync_symlink(self, data_path):
        if os.path.isfile(data_path.cache.relative):
            self.sync_to_cloud(data_path)
        else:
            self.sync_from_cloud(data_path)
        pass

    def sync_from_cloud(self, data_path):
        aws_key = self.cache_file_aws_key(data_path.cache.dvc)
        key = self._bucket.get_key(aws_key)
        if not key:
            raise DataSyncError('File "{}" does not exist in the cloud'.format(aws_key))

        Logger.printing('Downloading cache file from S3 "{}/{}"'.format(self._bucket.name,
                                                                        aws_key))
        key.get_contents_to_filename(data_path.cache.relative, cb=percent_cb)
        Logger.printing('Downloading completed')
        pass

    def sync_to_cloud(self, data_path):
        aws_key = self.cache_file_aws_key(data_path.cache.dvc)
        key = self._bucket.get_key(aws_key)
        if key:
            Logger.debug('File already uploaded to the cloud. Checksum validation...')

            md5_cloud = key.etag[1:-1]
            md5_local = file_md5(data_path.cache.relative)
            if md5_cloud == md5_local:
                Logger.debug('File checksum matches. No uploading is needed.')
                return

            Logger.printing('Checksum miss-match. Re-uploading is required.')

        Logger.printing('Uploading cache file "{}" to S3 "{}"'.format(data_path.cache.relative,
                                                                      aws_key))
        key = self._bucket.new_key(aws_key)
        key.set_contents_from_filename(data_path.cache.relative, cb=percent_cb)
        Logger.printing('Uploading completed')
        pass


if __name__ == '__main__':
    run(CmdDataSync())
