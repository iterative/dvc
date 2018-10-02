from dvc.command.base import CmdBase


class CmdGC(CmdBase):
    def run(self):
        msg = 'This will remove all cache except the cache that is used in '
        if not self.args.all_branches and not self.args.all_tags:
            msg += 'the current git branch.'
        elif self.args.all_branches and not self.args.all_tags:
            msg += 'all git branches.'
        elif not self.args.all_branches and self.args.all_tags:
            msg += 'all git tags.'
        else:
            msg += 'all git branches and all git tags.'

        self.project.logger.warn(msg)

        msg = 'Are you sure you want to proceed?'
        if not self.args.force and not self.project.prompt.prompt(msg):
            return 1

        self.project.gc(all_branches=self.args.all_branches,
                        all_tags=self.args.all_tags,
                        cloud=self.args.cloud,
                        remote=self.args.remote)
        return 0
