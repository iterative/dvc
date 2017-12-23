from dvc.command.common.base import CmdBase


class CmdRemove(CmdBase):
    def run(self):
        for target in self.args.targets:
            self.project.remove(target)
        return 0
