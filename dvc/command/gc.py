import os

from dvc.command.common.base import CmdBase
from dvc.logger import Logger
from dvc.config import ConfigI
from dvc.state_file import StateFile


class CmdGC(CmdBase):
    def __init__(self, settings):
        super(CmdGC, self).__init__(settings)

    def run(self):
        clist = [str(x) for x in StateFile.find_all_cache_files(self.git)]

        for cache in os.listdir(ConfigI.CACHE_DIR):
            fname = os.path.join(ConfigI.CACHE_DIR, cache)
            if os.path.basename(fname) in clist:
                continue
            os.remove(fname)
            Logger.info('Cache \'{}\' was removed'.format(fname))

        return 0
