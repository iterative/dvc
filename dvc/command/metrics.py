from dvc.command.base import CmdBase
from dvc.exceptions import DvcException


class CmdMetricsShow(CmdBase):
    def run(self):
        try:
            self.project.metrics_show(self.args.path,
                                      json_path=self.args.json_path,
                                      tsv_path=self.args.tsv_path,
                                      htsv_path=self.args.htsv_path,
                                      csv_path=self.args.csv_path,
                                      hcsv_path=self.args.hcsv_path,
                                      all_branches=self.args.all_branches)
        except DvcException as exc:
            self.project.logger.error('Failed to show metrics', exc)
            return 1

        return 0


class CmdMetricsAdd(CmdBase):
    def run(self):
        try:
            self.project.metrics_add(self.args.path)
        except DvcException as exc:
            msg = 'Failed to add metric file \'{}\''.format(self.args.path)
            self.project.logger.error(msg, exc)
            return 1

        return 0


class CmdMetricsRemove(CmdBase):
    def run(self):
        try:
            self.project.metrics_remove(self.args.path)
        except DvcException as exc:
            msg = 'Failed to remove metric file \'{}\''.format(self.args.path)
            self.project.logger.error(msg, exc)
            return 1

        return 0
