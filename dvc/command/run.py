from dvc.command.base import CmdBase
from dvc.exceptions import DvcException


class CmdRun(CmdBase):
    def run(self):
        try:
            self.project.run(cmd=' '.join(self.args.command),
                             outs=self.args.outs,
                             outs_no_cache=self.args.outs_no_cache,
                             metrics_no_cache=self.args.metrics_no_cache,
                             deps=self.args.deps,
                             fname=self.args.file,
                             cwd=self.args.cwd,
                             no_exec=self.args.no_exec)
        except DvcException as ex:
            self.project.logger.error('Failed to run command', ex)
            return 1

        return 0
