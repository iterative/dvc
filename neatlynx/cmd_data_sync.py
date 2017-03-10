import hashlib
import os

from boto.s3.connection import S3Connection

from neatlynx.cmd_base import CmdBase, Logger
from neatlynx.data_file_obj import DataFileObjExisting
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
        if os.path.islink(self.args.target):
            dobj = DataFileObjExisting(self.args.target, self.git, self.config)
            return self.sync_symlink(dobj)

        if os.path.isdir(self.args.target):
            return self.sync_dir(self.args.target)

        raise DataSyncError('File "{}" does not exit'.format(target))

    def sync_dir(self, dir):
        for f in os.listdir(dir):
            fname = os.path.join(dir, f)
            if os.path.isdir(fname):
                self.sync_dir(fname)
            elif os.path.islink(fname):
                self.sync_symlink(DataFileObjExisting(fname, self.git, self.config))
            else:
                raise DataSyncError('Unsupported file type "{}"'.format(fname))
        pass

    def sync_symlink(self, dobj):
        if os.path.isfile(dobj.cache_file_relative):
            self.sync_to_cloud(dobj)
        else:
            self.sync_from_cloud(dobj)
        pass

    def sync_from_cloud(self, dobj):
        key = self._bucket.get_key(dobj.cache_file_aws_key)
        if not key:
            raise DataSyncError('File "{}" does not exist in the cloud'.format(dobj.cache_file_aws_key))

        Logger.info('Downloading cache file from S3 "{}/{}"'.format(self._bucket.name,
                                                                    dobj.cache_file_aws_key))
        key.get_contents_to_filename(dobj.cache_file_relative, cb=percent_cb)
        Logger.info('Downloading completed')
        pass

    def sync_to_cloud(self, dobj):
        key = self._bucket.get_key(dobj.cache_file_aws_key)
        if key:
            Logger.verbose('File already uploaded to the cloud. Checksum validation...')

            md5_cloud = key.etag[1:-1]
            md5_local = file_md5(dobj.cache_file_relative)
            if md5_cloud == md5_local:
                Logger.verbose('File checksum matches. No uploading is needed.')
                return

            Logger.info('Checksum miss-match. Re-uploading is required.')

        Logger.info('Uploading cache file "{}" to S3 "{}"'.format(dobj.cache_file_relative,
                                                                  dobj.cache_file_aws_key))
        key = self._bucket.new_key(dobj.cache_file_aws_key)
        key.set_contents_from_filename(dobj.cache_file_relative, cb=percent_cb)
        Logger.info('Uploading completed')
        pass


if __name__ == '__main__':
    import sys
    try:
        sys.exit(CmdDataSync().run())
    except NeatLynxException as e:
        Logger.error(e)
        sys.exit(1)
