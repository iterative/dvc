from dvc.command.common.base import CmdBase


class CmdGC(CmdBase):
    def run(self):
        self.project.gc(all_branches=self.args.all_branches,
                        cloud=self.args.cloud,
                        remote=self.args.remote)
        return 0
