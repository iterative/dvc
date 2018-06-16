from dvc.command.common.base import CmdBase


class CmdMetricsShow(CmdBase):
    def run(self):
        self.project.metrics_show(self.args.path,
                                  json_path=self.args.json_path,
                                  tsv_path=self.args.tsv_path,
                                  htsv_path=self.args.htsv_path,
                                  all_branches=self.args.all_branches)
        return 0


class CmdMetricsAdd(CmdBase):
    def run(self):
        self.project.metrics_add(self.args.path)
        return 0


class CmdMetricsRemove(CmdBase):
    def run(self):
        self.project.metrics_remove(self.args.path)
        return 0
