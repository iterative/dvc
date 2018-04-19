from dvc.command.common.base import CmdBase


class CmdMetrics(CmdBase):
    def run(self):
        for branch, metric in self.project.metrics(self.args.path,
                                                   json_path=self.args.json_path,
                                                   tsv_path=self.args.tsv_path,
                                                   htsv_path=self.args.htsv_path).items():
            self.project.logger.info("{}: {}".format(branch, metric))
        return 0
