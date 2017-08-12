from dvc.command.base import CmdBase, DvcLock

class CmdDataSync(CmdBase):
    def __init__(self, settings):
        super(CmdDataSync, self).__init__(settings)

    def run(self):
        with DvcLock(self.is_locker, self.git):
            self.cloud.sync(self.parsed_args.targets, self.parsed_args.jobs)


class CmdDataPull(CmdBase):
    def __init__(self, settings):
        super(CmdDataPull, self).__init__(settings)

    def run(self):
        with DvcLock(self.is_locker, self.git):
            self.cloud.pull(self.parsed_args.targets, self.parsed_args.jobs)


class CmdDataPush(CmdBase):
    def __init__(self, settings):
        super(CmdDataPush, self).__init__(settings)

    def run(self):
        with DvcLock(self.is_locker, self.git):
            self.cloud.push(self.parsed_args.targets, self.parsed_args.jobs)
