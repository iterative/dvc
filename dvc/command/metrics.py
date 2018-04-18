from dvc.command.common.base import CmdBase


class CmdMetrics(CmdBase):
    def run(self):
        for branch, metric in self.project.metrics(self.args.path).items():
            self.project.logger.info("{}: {}".format(branch, metric))
        return 0
