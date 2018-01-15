import os
import stat

from dvc.command.common.base import CmdBase
from dvc.logger import Logger


class CmdCheckout(CmdBase):
    def run(self):
        self.remove_untracked_hardlinks()
        self.project.checkout()
        return 0

    def remove_untracked_hardlinks(self):
        untracked = self.project.scm.untracked_files()

        for file in self.project.cache.find_cache(untracked).keys():
            Logger.info(u'Remove \'{}\''.format(file))
            os.chmod(file, stat.S_IWRITE)
            os.remove(file)

            dir = os.path.dirname(file)
            if len(dir) != 0 and not os.listdir(dir):
                Logger.info(u'Remove empty directory \'{}\''.format(dir))
                os.removedirs(dir)
        pass
