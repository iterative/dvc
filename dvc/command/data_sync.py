import os

import dvc.cloud.base as cloud

from dvc.command.common.base import CmdBase
from dvc.exceptions import DvcException


class CmdDataBase(CmdBase):
    def do_run(self, target):
        pass

    def run(self):
        if not self.args.targets:
            return self.do_run()

        ret = 0
        for target in self.args.targets:
            if self.do_run(target):
                ret = 1
        return ret

class CmdDataPull(CmdDataBase):
    def do_run(self, target=None):
        try:
            self.project.pull(target=target, jobs=self.args.jobs)
        except DvcException as exc:
            self.project.logger.error('Failed to pull data from the cloud', exc)
            return 1
        return 0


class CmdDataPush(CmdDataBase):
    def do_run(self, target=None):
        try:
            self.project.push(target=target, jobs=self.args.jobs)
        except DvcException as exc:
            self.project.logger.error('Failed to push data to the cloud', exc)
            return 1
        return 0


class CmdDataStatus(CmdDataBase):
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

            path = os.path.relpath(target, self.project.cache.cache_dir)

            self.project.logger.info('\t{}\t{}'.format(prefix_map[ret], path))

    def do_run(self, target=None):
        try:
            status = self.project.status(target=target, jobs=self.args.jobs)
            self._show(status)
        except DvcException as exc:
            self.project.logger.error('Failed to obtain data status', exc)
            return 1
        return 0
