import os

import dvc.cloud.base as cloud

from dvc.command.common.base import CmdBase
from dvc.exceptions import DvcException


class CmdDataPull(CmdBase):
    def run(self):
        try:
            self.project.pull(self.args.jobs)
        except DvcException as exc:
            self.project.logger.error('Failed to pull data from the cloud', exc)
            return 1
        return 0


class CmdDataPush(CmdBase):
    def run(self):
        try:
            self.project.push(self.args.jobs)
        except DvcException as exc:
            self.project.logger.error('Failed to push data to the cloud', exc)
            return 1
        return 0


class CmdDataStatus(CmdBase):
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

            path = os.path.relpath(target)

            self.project.logger.info('\t{}\t{}'.format(prefix_map[ret], path))

    def run(self):
        try:
            status = self.project.status(self.args.jobs)
            self._show(status)
        except DvcException as exc:
            self.project.logger.error('Failed to obtain data status', exc)
            return 1
        return 0
