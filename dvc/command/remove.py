import os

import fasteners
from boto.s3.connection import S3Connection

from dvc.command.base import CmdBase
from dvc.exceptions import DvcException
from dvc.logger import Logger
from dvc.runtime import Runtime


class DataRemoveError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Data remove error: {}'.format(msg))


class CmdDataRemove(CmdBase):
    def __init__(self, settings):
        super(CmdDataRemove, self).__init__(settings)

    def define_args(self, parser):
        self.set_no_git_actions(parser)

        parser.add_argument('target', metavar='', help='Target to remove - file or directory.', nargs='*')
        parser.add_argument('-r', '--recursive', action='store_true', help='Remove directory recursively.')
        parser.add_argument('-l', '--keep-in-cloud', action='store_true', default=False,
                            help='Do not remove data from cloud.')
        parser.add_argument('-c', '--keep-in-cache', action='store_false', default=False,
                            help='Do not remove data from cache.')
        pass

    def run(self):
        if self.is_locker:
            lock = fasteners.InterProcessLock(self.git.lock_file)
            gotten = lock.acquire(timeout=5)
            if not gotten:
                Logger.info('[Cmd-Remove] Cannot perform the cmd since DVC is busy and locked. Please retry the cmd later.')
                return 1

        try:
            if not self.remove_all_targets():
                return 1
        finally:
            if self.is_locker:
                lock.release()

        return 0

    def remove_all_targets(self):
        if not self.no_git_actions and not self.git.is_ready_to_go():
            return False

        error = False
        for target in self.parsed_args.target:
            if not self.remove_target(target):
                error = True

        message = 'DVC data remove: {}'.format(' '.join(self.parsed_args.target))
        self.commit_if_needed(message, error)

        return error == 0

    def remove_target(self, target):
        try:
            if os.path.isdir(target):
                self.remove_dir(target)
            else:
                self.remove_file(target)
            return True
        except DvcException as ex:
            Logger.error('[Cmd-Remove] Unable to remove data item "{}": {}'.format(target, ex))
            return False

    def remove_dir(self, target):
        if not self.parsed_args.recursive:
            raise DataRemoveError('[Cmd-Remove] Directory "%s" cannot be removed. Use --recurcive flag.' % target)

        data_item = self.settings.path_factory.data_item(target)
        if data_item.data_dvc_short == '':
            raise DataRemoveError('[Cmd-Remove] Data directory "%s" cannot be removed' % target)

        return self.remove_dir_file_by_file(target)

    @staticmethod
    def remove_dir_if_empty(file):
        dir = os.path.dirname(file)
        if dir != '' and not os.listdir(dir):
            Logger.debug(u'[Cmd-Remove] Empty directory was removed {}.'.format(dir))
            os.rmdir(dir)
        pass

    def remove_file(self, target):
        # it raises exception if not a symlink is provided
        Logger.debug(u'[Cmd-Remove] Remove file {}.'.format(target))

        data_item = self.settings.path_factory.existing_data_item(target)

        self._remove_cache_file(data_item)
        self._remove_state_file(data_item)
        self._remove_cloud_cache(data_item)

        os.remove(data_item.data.relative)
        Logger.debug(u'[Cmd-Remove] Remove data item {}. Success.'.format(data_item.data.relative))
        pass

    def _remove_cloud_cache(self, data_item):
        if not self.parsed_args.keep_in_cloud:
            aws_key = self.cache_file_key(data_item.cache.dvc)
            self.remove_from_cloud(aws_key)

    def _remove_state_file(self, data_item):
        if os.path.isfile(data_item.state.relative):
            self._remove_dvc_path(data_item.state, 'state')
        else:
            Logger.warn(u'[Cmd-Remove] State file {} for data instance {} does not exist'.format(
                data_item.state.relative, data_item.data.relative))

    def _remove_cache_file(self, data_item):
        if not self.parsed_args.keep_in_cache and os.path.isfile(data_item.cache.relative):
            self._remove_dvc_path(data_item.cache, 'cache')
        else:
            if not self.parsed_args.keep_in_cache:
                msg = u'[Cmd-Remove] Unable to find cache file {} for data item {}'
                Logger.warn(msg.format(data_item.cache.relative, data_item.data.relative))
        pass

    def _remove_dvc_path(self, dvc_path, name):
        Logger.debug(u'[Cmd-Remove] Remove {} {}.'.format(name, dvc_path.relative))
        os.remove(dvc_path.relative)
        self.remove_dir_if_empty(dvc_path.relative)
        Logger.debug(u'[Cmd-Remove] Remove {}. Success.'.format(name))

    def remove_from_cloud(self, aws_file_name):
        Logger.debug(u'[Cmd-Remove] Remove from cloud {}.'.format(aws_file_name))

        if not self.config.aws_access_key_id or not self.config.aws_secret_access_key:
            Logger.debug('[Cmd-Remove] Unable to check cache file in the cloud')
            return
        conn = S3Connection(self.config.aws_access_key_id, self.config.aws_secret_access_key)
        bucket_name = self.config.storage_bucket
        bucket = conn.lookup(bucket_name)
        if bucket:
            key = bucket.get_key(aws_file_name)
            if not key:
                Logger.warn('[Cmd-Remove] S3 remove warning: file "{}" does not exist in S3'.format(aws_file_name))
            else:
                key.delete()
                Logger.info('[Cmd-Remove] File "{}" was removed from S3'.format(aws_file_name))
        pass

    def remove_dir_file_by_file(self, target):
        for f in os.listdir(target):
            file = os.path.join(target, f)
            if os.path.isdir(file):
                self.remove_dir_file_by_file(file)
            else:
                self.remove_file(file)

        os.rmdir(target)
        pass


if __name__ == '__main__':
    Runtime.run(CmdDataRemove)
