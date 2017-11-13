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
            try:
                states = self.git.state_files_for_previous_commit()
                prev_items = self.settings.path_factory.data_items_from_states(states, existing=False)
            except Exception as ex:
                Logger.error(u'Unable to get data files from previous commit'.format(ex))
                return 1

            curr_items = self.settings.path_factory.all_existing_data_items()
            self.checkout(curr_items)

            self.remove_files(list(set(prev_items) - set(curr_items)))
            return 0

    @staticmethod
    def remove_files(removed_items_set):
        for item in removed_items_set:
            path = item.data.relative
            if not os.path.exists(path):
                print(u'Remove \'{}\' - file does not exist'.format(path))
            else:
                Logger.info(u'Remove \'{}\''.format(path))
                os.remove(path)
                dir = os.path.dirname(path)
                if not os.listdir(dir):
                    Logger.info(u'Remove directory \'{}\''.format(dir))
                    os.removedirs(dir)
        pass
