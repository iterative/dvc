from dvc.command.base import CmdBase


class CmdCheckout(CmdBase):
    def run(self):
        if not self.args.targets:
            self.project.checkout(force=self.args.force)
        else:
            for target in self.args.targets:
                self.project.checkout(target=target,
                                      with_deps=self.args.with_deps,
                                      force=self.args.force)
        return 0
