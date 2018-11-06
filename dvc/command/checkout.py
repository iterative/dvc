from dvc.command.base import CmdBase


class CmdCheckout(CmdBase):
    def run(self):
        if not self.args.targets:
            self.project.checkout()
        else:
            for target in self.args.targets:
                self.project.checkout(target=target,
                                      with_deps=self.args.with_deps)
        return 0
