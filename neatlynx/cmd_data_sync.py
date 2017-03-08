import hashlib
import os

from boto.s3.connection import S3Connection

from neatlynx.cmd_base import CmdBase, Logger
from neatlynx.exceptions import NeatLynxException


class DataSyncError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'Data sync error: {}'.format(msg))


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)


def percent_cb(complete, total):
    Logger.verbose('{} transferred out of {}'.format(sizeof_fmt(complete), sizeof_fmt(total)))


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
            Logger.info('S3 bucket "{}" was created'.format(bucket_name))
        pass

    def define_args(self, parser):
        self.add_string_arg(parser, 'target', 'Target to sync - file or directory')
        pass

    def run(self):
        target = self.args.target
        rel_data_path = os.path.join(os.path.realpath(self.git.git_dir), self.config.data_dir)
        if not os.path.abspath(target).startswith(os.path.realpath(rel_data_path)):
            raise DataSyncError('File supposes to be in data dir - "{}"'.
                                format(self.config.data_dir))

        if os.path.islink(target):
            return self.sync_symlink(target)

        if os.path.isdir(target):
            return self.sync_dir(target)

        raise DataSyncError('File "{}" does not exit'.format(target))

    def sync_dir(self, dir):
        for f in os.listdir(dir):
            fname = os.path.join(dir, f)
            if os.path.isdir(fname):
                self.sync_dir(fname)
            elif os.path.islink(fname):
                self.sync_symlink(fname)
            else:
                raise DataSyncError('Unsupported file type "{}"'.format(fname))
        pass

    def sync_symlink(self, file):
        cache_file_rel_data = os.path.join(os.path.dirname(file), os.readlink(file))
        cache_file = os.path.relpath(os.path.realpath(cache_file_rel_data), os.path.realpath(os.curdir))

        if os.path.isfile(cache_file):
            self.sync_to_cloud(cache_file)
        else:
            self.sync_from_cloud(cache_file)
            pass
        pass

    def sync_from_cloud(self, cache_file):
        s3_file = self.get_cache_file_s3_name(cache_file)
        key = self._bucket.get_key(s3_file)
        if not key:
            raise DataSyncError('File "{}" is not exist in the cloud'.format(cache_file))

        Logger.info('Downloading cache file "{}" from S3 {}/{}'.format(cache_file, self._bucket.name, s3_file))
        key.get_contents_to_filename(cache_file, cb=percent_cb)
        Logger.info('Downloading completed')
        pass

    def sync_to_cloud(self, cache_file):
        target_file = self.get_cache_file_s3_name(cache_file)

        key = self._bucket.get_key(target_file)
        if key:
            Logger.verbose('File already uploaded to the cloud. Checking checksum...')

            md5_cloud = key.etag[1:-1]
            md5_local = file_md5(cache_file)
            if md5_cloud == md5_local:
                Logger.verbose('File checksum matches. No uploading is needed.')
                return

            Logger.info('Checksum miss-match. Re-uploading is required.')

        Logger.info('Uploading cache file "{}" to S3 {}/{}'.format(cache_file, self._bucket.name, target_file))
        key = self._bucket.new_key(target_file)
        key.set_contents_from_filename(cache_file, cb=percent_cb)
        Logger.info('Uploading completed')
        pass


if __name__ == '__main__':
    import sys
    try:
        sys.exit(CmdDataSync().run())
    except NeatLynxException as e:
        Logger.error(e)
        sys.exit(1)
