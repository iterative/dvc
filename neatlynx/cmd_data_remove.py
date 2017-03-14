import os
from boto.s3.connection import S3Connection
import fasteners

from neatlynx.cmd_base import CmdBase
from neatlynx.logger import Logger
from neatlynx.exceptions import NeatLynxException
from neatlynx.data_file_obj import DataFileObjExisting


class DataRemoveError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'Data remove error: {}'.format(msg))


class CmdDataRemove(CmdBase):
    def __init__(self):
        CmdBase.__init__(self)
        pass

    def define_args(self, parser):
        self.set_skip_git_actions(parser)

        parser.add_argument('target', metavar='', help='Target to remove - file or directory', nargs='*')
        parser.add_argument('-r', '--recursive', action='store_true', help='Remove directory recursively')
        parser.add_argument('-c', '--remove-from-cloud', action='store_true', help='Keep file in cloud')
        pass

    def run(self):
        lock = fasteners.InterProcessLock(self.git.lock_file)
        gotten = lock.acquire(timeout=5)
        if not gotten:
            Logger.info('Cannot perform the command since NLX is busy and locked. Please retry the command later.')
            return 1

        try:
            if not self.skip_git_actions and not self.git.is_ready_to_go():
                return 1

            for target in self.args.target:
                self.remove_target(target)

            if self.skip_git_actions:
                self.not_committed_changes_warning()
                return 0

            message = 'NLX data remove: {}'.format(' '.join(self.args.target))
            self.git.commit_all_changes_and_log_status(message)
        finally:
            lock.release()

    def remove_target(self, target):
        if os.path.isdir(target):
            if not self.args.recursive:
                raise DataRemoveError('Directory cannot be removed. Use --recurcive flag.')

            if os.path.realpath(target) == \
                    os.path.realpath(os.path.join(self.git.git_dir_abs, self.config.data_dir)):
                raise DataRemoveError('data directory cannot be removed')

            return self.remove_dir(target)
        dobj = DataFileObjExisting(target, self.git, self.config)
        if os.path.islink(dobj.data_file_relative):
            return self.remove_symlink(dobj.data_file_relative)

        raise DataRemoveError('Cannot remove a regular file "{}"'.format(target))

    def remove_symlink(self, file):
        dobj = DataFileObjExisting(file, self.git, self.config)

        if os.path.isfile(dobj.cache_file_relative):
            os.remove(dobj.cache_file_relative)
            dobj.remove_cache_dir_if_empty()

        if os.path.isfile(dobj.state_file_relative):
            os.remove(dobj.state_file_relative)
            dobj.remove_state_dir_if_empty()

        if self.args.remove_from_cloud:
            self.remove_from_cloud(dobj.cache_file_aws_key)

        os.remove(file)
        pass

    def remove_from_cloud(self, aws_file_name):
        conn = S3Connection(self.config.aws_access_key_id, self.config.aws_secret_access_key)
        bucket_name = self.config.aws_storage_bucket
        bucket = conn.lookup(bucket_name)
        if bucket:
            key = bucket.get_key(aws_file_name)
            if not key:
                Logger.warn('S3 remove warning: file "{}" does not exist in S3'.format(aws_file_name))
            else:
                key.delete()
                Logger.info('File "{}" was removed from S3'.format(aws_file_name))

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
        pass


if __name__ == '__main__':
    import sys
    try:
        sys.exit(CmdDataRemove().run())
    except NeatLynxException as e:
        Logger.error(e)
        sys.exit(1)
