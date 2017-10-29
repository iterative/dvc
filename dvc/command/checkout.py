import os
from stat import ST_INO, ST_DEV

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

        data_st = os.stat(data)
        cache_st = os.stat(cache)

        if (data_st[ST_INO], data_st[ST_DEV]) != (cache_st[ST_INO], cache_st[ST_DEV]):
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
            items = self.settings.path_factory.all_existing_data_items()
            self.checkout(items)
            return 0
