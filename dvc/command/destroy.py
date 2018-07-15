from dvc.command.common.base import CmdBase


class CmdDestroy(CmdBase):
    def run(self):
        try:
            self.project.destroy()
        except Exception as exc:
            self.project.logger.error('Failed to destroy DVC', exc)
            return 1
        return 0
