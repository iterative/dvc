import os

from dvc.command.common.base import CmdBase
from dvc.config import ConfigI
from dvc.logger import Logger
from dvc.state_file import StateFile
from dvc.path.data_item import DataItem
from dvc.system import System
from dvc.command.checkout import CmdCheckout


class CmdMerge(CmdBase):
    def __init__(self, settings):
        super(CmdMerge, self).__init__(settings)

    def print_info(self, targets):
        for fname in targets:
            Logger.info('Restored original data after merge:')
            Logger.info(' {}'.format(fname))

    def collect_targets(self):
        targets = []

        for fname in self.git.get_last_merge_changed_files():
            if not StateFile._is_state_file(fname):
                continue

            state = StateFile.load(fname)
            if not state.cmd and state.locked:
                targets.append(fname)

        return targets

    def checkout_targets(self, targets):
        items = []
        for fname in targets:
            self.git.checkout_file_before_last_merge(fname)
            state = StateFile.load(fname)
            for out in state.out:
                item = self.settings.path_factory.data_item(os.path.join(state.cwd, out))
                items.append(item)

        CmdCheckout.checkout(items)

        msg = 'DVC merge files: {}'.format(' '.join(targets))
        self.commit_if_needed(msg)

    def run(self):
        targets = self.collect_targets()
        if not targets:
            return 1

        self.checkout_targets(targets)
        self.print_info(targets)

        return 0
