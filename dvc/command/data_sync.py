import dvc.cloud.data_cloud as cloud
from dvc.command.common.base import CmdBase

class CmdDataPull(CmdBase):
    def run(self):
        self.project.pull(self.args.jobs)


class CmdDataPush(CmdBase):
    def run(self):
        self.project.push(self.args.jobs)


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

            self.project.logger.info('\t{}\t{}'.format(prefix_map[ret], target))

    def run(self):
        status = self.project.status(self.args.jobs)
        self._show(status)
        return 0
