import os

from dvc.command.common.base import CmdBase
from dvc.logger import Logger
from dvc.system import System


class CmdCheckout(CmdBase):
    def run(self):
        self.remove_untracked_hardlinks()
        self.project.checkout()
        return 0

    def remove_untracked_hardlinks(self):
        untracked_cache_files = self.untracked_hardlinks_files()

        for file in untracked_cache_files:
            Logger.info(u'Remove \'{}\''.format(file))
            os.remove(file)

            dir = os.path.dirname(file)
            if not os.listdir(dir):
                Logger.info(u'Remove empty directory \'{}\''.format(dir))
                os.removedirs(dir)
        pass

    def untracked_hardlinks_files(self):
        untracked_files = set(self.project.scm.untracked_files())
        cached_files = []
        for cache_file in self.project.cache.all():
            hardlinks = list(filter(lambda f: System.samefile(cache_file, f), untracked_files))
            cached_files.extend(hardlinks)
            untracked_files = untracked_files - set(hardlinks)

        return cached_files
