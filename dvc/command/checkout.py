import os

from dvc.command.base import CmdBase, DvcLock
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

    def checkout(self, items):
        for item in items:
            if self.cache_ok(item):
                continue

            if os.path.isfile(item.data.relative):
                os.remove(item.data.relative)

            System.hardlink(item.cache.relative, item.data.relative)
            Logger.info('Checkout \'{}\''.format(item.data.relative))
 
    def run(self):
        with DvcLock(self.is_locker, self.git):
            curr_commit = self.git.curr_branch_or_commit
            prev_items = []
            try:
                self.git.checkout_previous()
                prev_items = self.settings.path_factory.all_existing_data_items()
            except Exception as ex:
                Logger.error(u'Unable to get '.format(ex))
                return 1
            finally:
                self.git.checkout(curr_commit)

            curr_items = self.settings.path_factory.all_existing_data_items()
            self.checkout(curr_items)

            self.remove_files(list(set(prev_items) - set(curr_items)))
            return 0

    @staticmethod
    def remove_files(removed_items_set):
        for item in removed_items_set:
            Logger.info(u'Remove \'{}\''.format(item.data.relative))
            os.remove(item.data.relative)
            dir = os.path.dirname(item.data.relative)
            if not os.listdir(dir):
                Logger.info(u'Remove directory \'{}\''.format(dir))
                os.removedirs(dir)
        pass
