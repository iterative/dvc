import os

from dvc.command.common.base import CmdBase
from dvc.command.common.cache_dir import CacheDir
from dvc.config import ConfigI
from dvc.logger import Logger
from dvc.system import System


class CmdCheckout(CmdBase):
    def __init__(self, settings):
        super(CmdCheckout, self).__init__(settings)

    @staticmethod
    def cache_ok(item):
        data = item.data.relative
        cache = item.cache.relative

        if not os.path.isfile(data) or not os.path.isfile(cache):
            return False

        if not System.samefile(data, cache):
            return False

        return True

    @staticmethod
    def checkout(items):
        for item in items:
            if CmdCheckout.cache_ok(item):
                continue

            if os.path.isfile(item.data.relative):
                os.remove(item.data.relative)

            System.hardlink(item.cache.relative, item.data.relative)
            Logger.info('Checkout \'{}\''.format(item.data.relative))
 
    def run(self):
        self.remove_not_tracked_hardlinks()
        items = self.settings.path_factory.all_existing_data_items()
        self.checkout(items)
        return 0

    def remove_not_tracked_hardlinks(self):
        untracked_files = self.git.all_untracked_files()

        cache_dir = os.path.join(self.git.git_dir_abs, ConfigI.CACHE_DIR)
        cached_files = CacheDir(cache_dir).find_caches(untracked_files)

        for file in cached_files:
            Logger.info(u'Remove \'{}\''.format(file))
            os.remove(file)

            dir = os.path.dirname(file)
            if not os.listdir(dir):
                Logger.info(u'Remove empty directory \'{}\''.format(dir))
                os.removedirs(dir)
        pass
