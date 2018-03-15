import os

import dvc.cloud.base as cloud

from dvc.command.data_sync import CmdDataBase


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
        except Exception as exc:
            self.project.logger.error('Failed to obtain data status', exc)
            return 1
        return 0
