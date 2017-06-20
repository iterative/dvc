import os

from dvc.command.traverse import Traverse
from dvc.logger import Logger
from dvc.path.data_item import DataItemError
from dvc.runtime import Runtime


class CmdRemove(Traverse):
    def __init__(self, settings):
        super(CmdRemove, self).__init__(settings, "remove")

    def define_args(self, parser):
        super(CmdRemove, self).define_args(parser)
        parser.add_argument('-r', '--recursive', action='store_true', help='Remove directory recursively.')
        parser.add_argument('-c', '--keep-in-cache', action='store_false', default=False,
                            help='Do not remove data from cache.')
        pass

    def process_file(self, target):
        Logger.debug(u'[Cmd-Remove] Remove file {}.'.format(target))

        try:
            data_item = self.settings.path_factory.existing_data_item(target)
        except DataItemError:
            Logger.warn(u'[Cmd-Remove] Data file {} is not valid symbolic link'.format(target))
            data_item = self.settings.path_factory.data_item(target)

        self._remove_cache_file(data_item)
        self._remove_state_file(data_item)
        self._remove_cloud_cache(data_item)

        os.remove(data_item.data.relative)
        Logger.debug(u'[Cmd-Remove] Remove data item {}. Success.'.format(data_item.data.relative))
        pass

    def _remove_cloud_cache(self, data_item):
        if not self.parsed_args.keep_in_cloud:
            self.cloud.remove_from_cloud(data_item)

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

    @staticmethod
    def remove_dir_if_empty(file):
        dir = os.path.dirname(file)
        if dir != '' and not os.listdir(dir):
            Logger.debug(u'[Cmd-Remove] Empty directory was removed {}.'.format(dir))
            os.rmdir(dir)
        pass

    def traverse_dir_finalize(self, target):
        os.rmdir(target)

    def is_recursive(self):
        return self.parsed_args.recursive

    # Renaming
    def remove_dir(self, target):
        return self._traverse_dir(target)

    def remove_file(self, target):
        self.process_file(target)

    def remove_all(self):
        return self._traverse_all()


if __name__ == '__main__':
    Runtime.run(CmdRemove)
