from dvc.command.base import CmdBase


class CmdDataBase(CmdBase):
    def do_run(self, target):
        pass

    def run(self):
        if not self.args.targets:
            return self.do_run()

        ret = 0
        for target in self.args.targets:
            if self.do_run(target):
                ret = 1
        return ret


class CmdDataPull(CmdDataBase):
    def do_run(self, target=None):
        try:
            self.project.pull(target=target,
                              jobs=self.args.jobs,
                              remote=self.args.remote,
                              show_checksums=self.args.show_checksums,
                              all_branches=self.args.all_branches,
                              with_deps=self.args.with_deps)
        except Exception as exc:
            self.project.logger.error('Failed to pull data from the cloud',
                                      exc)
            return 1
        return 0


class CmdDataPush(CmdDataBase):
    def do_run(self, target=None):
        try:
            self.project.push(target=target,
                              jobs=self.args.jobs,
                              remote=self.args.remote,
                              show_checksums=self.args.show_checksums,
                              all_branches=self.args.all_branches,
                              with_deps=self.args.with_deps)
        except Exception as exc:
            self.project.logger.error('Failed to push data to the cloud', exc)
            return 1
        return 0


class CmdDataFetch(CmdDataBase):
    def do_run(self, target=None):
        try:
            self.project.fetch(target=target,
                               jobs=self.args.jobs,
                               remote=self.args.remote,
                               show_checksums=self.args.show_checksums,
                               all_branches=self.args.all_branches,
                               with_deps=self.args.with_deps)
        except Exception as exc:
            self.project.logger.error('Failed to fetch data from the cloud',
                                      exc)
            return 1
        return 0
