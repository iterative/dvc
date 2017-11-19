import os

from dvc.command.traverse import Traverse
from dvc.logger import Logger
from dvc.path.data_item import DataItemError
from dvc.config import ConfigI


class CmdGC(Traverse):
    def __init__(self, settings):
        super(CmdGC, self).__init__(settings, "garbage collect", do_not_start_from_root=False)
        self.clist = []

    def run(self):
        if not self._traverse(self.git.git_dir_abs):
            Logger.error('Failed to collect used cache')
            return 1

        for cache in os.listdir(ConfigI.CACHE_DIR):
            fname = os.path.join(ConfigI.CACHE_DIR, cache)
            if fname in self.clist:
                continue
            os.remove(fname)
            self._remove_cloud_cache(self.settings.path_factory.data_item('.empty', fname))
            Logger.info('Cache \'{}\' was removed'.format(fname))

        return 0

    def process_file(self, target):
        Logger.debug(u'[Cmd-GC] GC file {}.'.format(target))

        try:
            data_item = self.settings.path_factory.existing_data_item(target)
        except DataItemError:
            return

        self.clist.append(data_item.cache.relative)
        Logger.debug(u'[Cmd-GC] GC data item {}. Success.'.format(data_item.data.relative))

    def is_recursive(self):
        return self.parsed_args.recursive

    @property
    def no_git_actions(self):
        return True

    @staticmethod
    def not_committed_changes_warning():
        pass
