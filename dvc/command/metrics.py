from dvc.command.base import CmdBase
from dvc.exceptions import DvcException


class CmdMetricsShow(CmdBase):
    def run(self):
        try:
            # backward compatibility
            if self.args.json_path:
                typ = 'json'
                xpath = self.args.json_path
            elif self.args.tsv_path:
                typ = 'tsv'
                xpath = self.args.tsv_path
            elif self.args.htsv_path:
                typ = 'htsv'
                xpath = self.args.htsv_path
            elif self.args.csv_path:
                typ = 'csv'
                xpath = self.args.csv_path
            elif self.args.hcsv_path:
                typ = 'hcsv'
                xpath = self.args.hcsv_path
            else:
                typ = self.args.type
                xpath = self.args.xpath

            self.project.metrics_show(self.args.path,
                                      typ=typ,
                                      xpath=xpath,
                                      all_branches=self.args.all_branches,
                                      all_tags=self.args.all_tags)
        except DvcException as exc:
            self.project.logger.error('Failed to show metrics', exc)
            return 1

        return 0


class CmdMetricsModify(CmdBase):
    def run(self):
        try:
            self.project.metrics_modify(self.args.path,
                                        typ=self.args.type,
                                        xpath=self.args.xpath)
        except DvcException as exc:
            self.project.logger.error('Failed to modify metrics', exc)
            return 1

        return 0


class CmdMetricsAdd(CmdBase):
    def run(self):
        try:
            self.project.metrics_add(self.args.path,
                                     self.args.type,
                                     self.args.xpath)
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
