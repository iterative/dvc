from dvc.command.common.base import CmdBase


class CmdRun(CmdBase):
    def run(self):
        self.project.run(cmd=' '.join(self.args.command),
                         outs=self.args.outs,
                         outs_no_cache=self.args.outs_no_cache,
                         deps=self.args.deps,
                         deps_no_cache=self.args.deps_no_cache,
                         locked=self.args.lock,
                         fname=self.args.file,
                         cwd=self.args.cwd)
        return 0
