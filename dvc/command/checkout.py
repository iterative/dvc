from dvc.command.base import CmdBase


class CmdCheckout(CmdBase):
    def run(self):
        if not self.args.targets:
            self.project.checkout()
        else:
            for target in self.args.targets:
                self.project.checkout(target=target)
        return 0
