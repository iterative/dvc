import os

from dvc.command.traverse import Traverse
from dvc.logger import Logger


class CmdGC(Traverse):
    def __init__(self, settings):
        super(CmdGC, self).__init__(settings, "garbage collect", do_not_start_from_root=False)

    def process_file(self, target):
        Logger.debug(u'[Cmd-GC] GC file {}.'.format(target))

        data_item = self._get_data_item(target)

        for cache_data_item in data_item.get_all_caches():
            if cache_data_item.cache.relative != data_item.resolved_cache.relative:
                os.remove(cache_data_item.cache.relative)
                Logger.info(u'GC cache file {} was removed'.format(cache_data_item.cache.relative))
                self._remove_cloud_cache(cache_data_item)

        Logger.debug(u'[Cmd-GC] GC data item {}. Success.'.format(data_item.data.relative))
        pass

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

    @property
    def no_git_actions(self):
        return True

    @staticmethod
    def not_committed_changes_warning():
        pass
