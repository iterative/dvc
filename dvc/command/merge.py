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
        for item in targets:
            Logger.info('Restored original data after merge:')
            Logger.info(' {}'.format(item.data.relative))

    def collect_data(self):
        dlist = []
        flist = self.git.get_last_merge_changed_files()
        for fname in flist:
            if fname.endswith(DataItem.STATE_FILE_SUFFIX):
                data = fname[:-len(DataItem.STATE_FILE_SUFFIX)]
                dlist.append(data)
        return dlist

    def collect_targets(self):
        targets = []
        flist = self.collect_data()
        items = self.settings.path_factory.to_data_items(flist)[0]

        for item in items:
            command = StateFile.load(item)

            if not command.cmd and command.locked:
                targets.append(item)

        return targets

    def checkout_targets(self, targets):
        data = []
        for item in targets:
            prev_state = StateFile.loads(self.git.get_file_content_before_last_merge(item.state.relative))
            curr_state = StateFile.load(item)

            curr_state.out[item.data.dvc] = prev_state.out[item.data.dvc]
            curr_state.save()

            CmdCheckout.checkout([item])

        msg = 'DVC merge files: {}'.format(' '.join(data))
        self.commit_if_needed(msg)

    def run(self):
        targets = self.collect_targets()
        if not targets:
            return 1

        self.checkout_targets(targets)
        self.print_info(targets)

        return 0
