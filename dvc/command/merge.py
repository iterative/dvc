import json

from dvc.command.base import CmdBase, DvcLock
from dvc.logger import Logger
from dvc.state_file import StateFile
from dvc.path.data_item import NotInDataDirError

class CmdMerge(CmdBase):
    def __init__(self, settings):
        super(CmdMerge, self).__init__(settings)

    def print_info(self, targets):
        for fname in targets:
            Logger.info('Restored original data after merge:')
            Logger.info(' {}'.format(fname))

    def collect_targets(self):
        targets = []
        flist = self.git.get_last_merge_changed_files()
        for fname in flist:
            try:
                item = self.settings.path_factory.data_item(fname)
            except NotInDataDirError:
                continue

            try:
                state = StateFile.load(item, self.git)
            except Exception as ex:
                Logger.error('Failed to load state file for {}'.format(fname), exc_info=True)
                return None

            if not state.command == StateFile.COMMAND_IMPORT_FILE:
                continue

            targets.append(fname)

        return targets

    def checkout_targets(self, targets):
        for fname in targets:
            self.git.checkout_file_before_last_merge(fname)

        msg = 'DVC merge files: {}'.format(' '.join(targets))
        self.commit_if_needed(msg)

    def run(self):
        with DvcLock(self.is_locker, self.git):
            targets = self.collect_targets()
            if not targets:
                return 1

            self.checkout_targets(targets)
            self.print_info(targets)

            return 0
