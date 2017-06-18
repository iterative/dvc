import os

from dvc.command.traverse import Traverse
from dvc.logger import Logger
from dvc.path.data_item import DataItemError
from dvc.runtime import Runtime


class CmdGC(Traverse):
    def __init__(self, settings):
        super(CmdGC, self).__init__(settings, "garbage collect", do_not_start_from_root=False)

    def define_args(self, parser):
        super(CmdGC, self).define_args(parser)
        parser.add_argument('-r', '--recursive', action='store_true', help='Remove directory recursively.')
        parser.add_argument('-c', '--keep-in-cache', action='store_false', default=False,
                            help='Do not remove data from cache.')
        pass

    def process_file(self, target):
        Logger.debug(u'[Cmd-GC] GC file {}.'.format(target))

        try:
            data_item = self.settings.path_factory.existing_data_item(target)
        except DataItemError:
            Logger.warn(u'[Cmd-GC] Data item {} is not a valid symbolic link'.format(target))
            data_item = self.settings.path_factory.data_item(target)

        for cache_data_item in data_item.get_all_caches():
            if cache_data_item.cache.relative != data_item.resolved_cache.relative:
                os.remove(cache_data_item.cache.relative)
                Logger.info(u'GC cache file {} was removed'.format(cache_data_item.cache.relative))
                self._remove_cloud_cache(cache_data_item)

        Logger.debug(u'[Cmd-GC] GC data item {}. Success.'.format(data_item.data.relative))
        pass

    def _remove_cloud_cache(self, data_item):
        if not self.parsed_args.keep_in_cloud:
            self.cloud.remove_from_cloud(data_item)

    def is_recursive(self):
        return self.parsed_args.recursive

    # Renaming
    def gc_dir(self, target):
        Logger.debug(u'[Cmd-GC] GC dir {}.'.format(target))
        return self._traverse_dir(target)

    def gc_file(self, target):
        self.process_file(target)

    def gc_all(self):
        return self._traverse_all()


if __name__ == '__main__':
    Runtime.run(CmdGC)
