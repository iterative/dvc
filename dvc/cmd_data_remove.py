import os

import fasteners
from boto.s3.connection import S3Connection

from dvc.cmd_base import CmdBase
from dvc.exceptions import DvcException
from dvc.logger import Logger
from dvc.utils import run


class DataRemoveError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Data remove error: {}'.format(msg))


class CmdDataRemove(CmdBase):
    def __init__(self, parse_config=True, git_obj=None, config_obj=None):
        super(CmdDataRemove, self).__init__(parse_config, git_obj, config_obj)
        pass

    def define_args(self, parser):
        self.set_skip_git_actions(parser)

        parser.add_argument('target', metavar='', help='Target to remove - file or directory', nargs='*')
        parser.add_argument('-r', '--recursive', action='store_true', help='Remove directory recursively')
        parser.add_argument('-l', '--keep-in-cloud', action='store_false', default=False,
                            help='Do not remove data from cloud')
        parser.add_argument('-c', '--keep-in-cache', action='store_false', default=False,
                            help='Do not remove data from cache')
        pass

    def run(self):
        lock = fasteners.InterProcessLock(self.git.lock_file)
        gotten = lock.acquire(timeout=5)
        if not gotten:
            Logger.info('Cannot perform the command since DVC is busy and locked. Please retry the command later.')
            return 1

        try:
            return self.remove_all_targets()
        finally:
            lock.release()

    def remove_all_targets(self):
        if not self.skip_git_actions and not self.git.is_ready_to_go():
            return 1

        error = False
        for target in self.args.target:
            if not self.remove_target(target):
                error = True

        message = 'DVC data remove: {}'.format(' '.join(self.args.target))
        self.commit_if_needed(message, error)

        return 0 if error == 0 else 1

    def remove_target(self, target):
        try:
            if os.path.isdir(target):
                self.remove_dir(target)
            else:
                self.remove_data_instance(target)
            return True
        except DvcException as ex:
            Logger.error('Unable to remove data file "{}": {}'.format(target, ex))
            return False

    def remove_dir(self, target):
        if not self.args.recursive:
            raise DataRemoveError('Directory "%s" cannot be removed. Use --recurcive flag.' % target)

        data_item = self.path_factory.data_item(target)
        if data_item.data_dvc_short == '':
            raise DataRemoveError('Data directory "%s" cannot be removed' % target)

        return self.remove_dir_file_by_file(target)

    @staticmethod
    def remove_dir_if_empty(file):
        dir = os.path.dirname(file)
        if dir != '' and not os.listdir(dir):
            os.rmdir(dir)
        pass

    def remove_data_instance(self, target):
        # it raises exception if not a symlink is provided
        data_item = self.path_factory.existing_data_item(target)

        self._remove_cache_file(data_item)
        self._remove_state_file(data_item)
        self._remove_cloud_cache(data_item)

        os.remove(data_item.data.relative)
        pass

    def _remove_cloud_cache(self, data_item):
        if not self.args.keep_in_cloud:
            aws_key = self.cache_file_aws_key(data_item.cache.dvc)
            self.remove_from_cloud(aws_key)

    def _remove_state_file(self, data_item):
        if os.path.isfile(data_item.state.relative):
            os.remove(data_item.state.relative)
            self.remove_dir_if_empty(data_item.state.relative)
        else:
            Logger.warn(u'State file {} for data instance {} does not exist'.format(
                data_item.state.relative, data_item.data.relative))

    def _remove_cache_file(self, data_item):
        if not self.args.keep_in_cache and os.path.isfile(data_item.cache.relative):
            os.remove(data_item.cache.relative)
            self.remove_dir_if_empty(data_item.cache.relative)
        else:
            if not self.args.keep_in_cache:
                Logger.warn(u'Unable to find cache file for data instance %s' % data_item.data.relative)

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
        pass

    def remove_dir_file_by_file(self, target):
        for f in os.listdir(target):
            file = os.path.join(target, f)
            if os.path.isdir(file):
                self.remove_dir_file_by_file(file)
            else:
                self.remove_data_instance(file)

        os.rmdir(target)
        pass


if __name__ == '__main__':
    run(CmdDataRemove())
