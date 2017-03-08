import os
from boto.s3.connection import S3Connection, Key

from neatlynx.cmd_base import CmdBase, Logger
from neatlynx.exceptions import NeatLynxException


class DataRemoveError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'Data remove error: {}'.format(msg))


class CmdDataRemove(CmdBase):
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
        self.add_string_arg(parser, 'target', 'Target to remove - file or directory')
        parser.add_argument('-r', '--recursive', action='store_true', help='Remove directory recursively')
        parser.add_argument('-k', '--keep-in-cloud', action='store_true', help='Keep file in cloud')
        pass

    def run(self):
        target = self.args.target

        if os.path.islink(target):
            return self.remove_symlink(target)

        if os.path.isdir(target):
            if not self.args.recursive:
                raise DataRemoveError('Directory cannot be removed. Use --recurcive flag.')

            if os.path.realpath(target) == os.path.realpath(self.config.data_dir):
                raise DataRemoveError('data directory cannot be removed')
            return self.remove_dir(target)

        raise DataRemoveError('File "{}" does not exit'.format(target))

    def remove_symlink(self, file):
        if not file.startswith(self.config.data_dir):
            raise DataRemoveError('File "{}" supposes to be in data dir'.format(file))

        cache_file_rel_data = os.path.join(os.path.dirname(file), os.readlink(file))
        cache_file = os.path.relpath(os.path.realpath(cache_file_rel_data), os.path.realpath(os.curdir))

        rel_data_file = os.path.relpath(file, self.config.data_dir)
        state_file = os.path.join(self.config.state_dir, rel_data_file)

        if os.path.isfile(cache_file):
            os.remove(cache_file)
            os.remove(file)

            if not os.path.isfile(state_file):
                Logger.warn('Warning: state file "{}" does not exist'.format(state_file))
            else:
                os.remove(state_file)

            if not self.args.keep_in_cloud:
                s3_name = self.get_cache_file_s3_name(cache_file)
                key = self._bucket.get_key(s3_name)
                if not key:
                    Logger.warn('S3 remove warning: file "{}" does not exist in S3'.format(s3_name))
                else:
                    key.delete()
                    Logger.info('File "{}" was removed from S3'.format(s3_name))
        pass

    def remove_dir(self, data_dir):
        for f in os.listdir(data_dir):
            fname = os.path.join(data_dir, f)
            if os.path.isdir(fname):
                self.remove_dir(fname)
            elif os.path.islink(fname):
                self.remove_symlink(fname)
            else:
                raise DataRemoveError('Unsupported file type "{}"'.format(fname))

        os.rmdir(data_dir)

        rel_data_dir = os.path.relpath(data_dir, self.config.data_dir)
        cache_dir = os.path.join(self.config.cache_dir, rel_data_dir)
        os.rmdir(cache_dir)
        pass


if __name__ == '__main__':
    import sys
    try:
        sys.exit(CmdDataRemove().run())
    except NeatLynxException as e:
        Logger.error(e)
        sys.exit(1)
