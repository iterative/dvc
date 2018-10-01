from dvc.command.base import CmdBase


class CmdGC(CmdBase):
    def run(self):
        self.project.gc(all_branches=self.args.all_branches,
                        all_tags=self.args.all_tags,
                        cloud=self.args.cloud,
                        remote=self.args.remote)
        return 0
