from dvc.command.common.base import CmdBase


class CmdCheckout(CmdBase):
    def run(self):
        self.project.checkout()
        return 0
