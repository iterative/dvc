from dvc.command.base import CmdBase, DvcLock

class CmdDataSync(CmdBase):
    def __init__(self, settings):
        super(CmdDataSync, self).__init__(settings)

    def run(self):
        with DvcLock(self.is_locker, self.git):
            self.settings.cloud.sync(self.parsed_args.targets, self.parsed_args.jobs)
