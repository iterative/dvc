import os

from dvc.command.common.base import CmdBase
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
        items = self.settings.path_factory.all_existing_data_items()
        self.checkout(items)
        return 0
