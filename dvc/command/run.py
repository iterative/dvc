from dvc.command.base import CmdBase
from dvc.exceptions import DvcException


class CmdRun(CmdBase):
    def run(self):
        try:
            if self.args.yes:
                self.project.prompt.default = True

            if len(self.args.outs) != len(set(self.args.outs)):
                s = set([x for x in self.args.outs if self.args.outs.count(x) > 1])
                for i in s:
                    print("Warning: Output \'" + i +
                          "\' was specified more than one time")
                self.args.outs = set(self.args.outs)

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
