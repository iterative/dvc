import os

from dvc.command.base import CmdBase


class CmdGC(CmdBase):
    def run(self):
        msg = 'This will remove all cache except the cache that is used in '
        if not self.args.all_branches and not self.args.all_tags:
            msg += 'the current git branch'
        elif self.args.all_branches and not self.args.all_tags:
            msg += 'all git branches'
        elif not self.args.all_branches and self.args.all_tags:
            msg += 'all git tags'
        else:
            msg += 'all git branches and all git tags'

        if self.args.projects is not None and len(self.args.projects) > 0:
            msg += ' of the current and the following projects:'

            for project_path in self.args.projects:
                msg += '\n  - %s' % os.path.abspath(project_path)
        else:
            msg += ' of the current project.'

        self.project.logger.warn(msg)

        msg = 'Are you sure you want to proceed?'
        if not self.args.force and not self.project.prompt.prompt(msg):
            return 1

        self.project.gc(all_branches=self.args.all_branches,
                        all_tags=self.args.all_tags,
                        cloud=self.args.cloud,
                        remote=self.args.remote,
                        force=self.args.force,
                        jobs=self.args.jobs,
                        projects=self.args.projects)
        return 0
