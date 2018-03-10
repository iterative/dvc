import os
import stat

from dvc.system import System
from dvc.command.common.base import CmdBase
from dvc.logger import Logger


class CmdCheckout(CmdBase):
    def run(self):
        self.remove_untracked_hardlinks()
        self.project.checkout()
        return 0

    def remove_untracked_hardlinks(self):
        untracked = self.project.scm.untracked_files()
        cache = dict((System.inode(c), c) for c in self.project.cache.all())
        for file in untracked:
            inode = System.inode(file)
            if inode not in cache.keys():
                continue

            Logger.info(u'Remove \'{}\''.format(file))
            os.chmod(file, stat.S_IWRITE)
            os.remove(file)
            os.chmod(cache[inode], stat.S_IREAD)

            dir = os.path.dirname(file)
            if len(dir) != 0 and not os.listdir(dir):
                Logger.info(u'Remove empty directory \'{}\''.format(dir))
                os.removedirs(dir)
        pass
