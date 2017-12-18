from dvc.command.common.base import CmdBase


class CmdAdd(CmdBase):
    def run(self):
        for target in self.args.targets:
            self.project.add(target)
        return 0
