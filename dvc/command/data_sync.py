from dvc.command.base import CmdBase, DvcLock
from dvc.logger import Logger

import dvc.data_cloud as cloud

class CmdDataSync(CmdBase):
    def __init__(self, settings):
        super(CmdDataSync, self).__init__(settings)

    def run(self):
        with DvcLock(self.is_locker, self.git):
            self.cloud.sync(self.parsed_args.targets, self.parsed_args.jobs)


class CmdDataPull(CmdBase):
    def __init__(self, settings):
        super(CmdDataPull, self).__init__(settings)

    def run(self):
        with DvcLock(self.is_locker, self.git):
            self.cloud.pull(self.parsed_args.targets, self.parsed_args.jobs)


class CmdDataPush(CmdBase):
    def __init__(self, settings):
        super(CmdDataPush, self).__init__(settings)

    def run(self):
        with DvcLock(self.is_locker, self.git):
            self.cloud.push(self.parsed_args.targets, self.parsed_args.jobs)

class CmdDataStatus(CmdBase):
    def __init__(self, settings):
        super(CmdDataStatus, self).__init__(settings)

    def _show(self, status):
        for s in status:
            target, ret = s

            if ret == cloud.STATUS_UNKNOWN or ret == cloud.STATUS_OK:
                continue

            prefix_map = {
                cloud.STATUS_DELETED  : 'deleted: ',
                cloud.STATUS_MODIFIED : 'modified:',
                cloud.STATUS_NEW      : 'new file:',
            }

            Logger.info('\t{}\t{}'.format(prefix_map[ret], target.resolved_cache.dvc))

    def run(self):
        with DvcLock(self.is_locker, self.git):
            status = self.cloud.status(self.parsed_args.targets, self.parsed_args.jobs)

            self._show(status)

            return 0
