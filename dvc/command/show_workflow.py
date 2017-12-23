from dvc.command.common.base import CmdBase
from dvc.git_wrapper import GitWrapper


class CmdShowWorkflow(CmdBase):
    def run(self):
        target = self.args.target
        if not target:
            target = self.project.config._config['Global'].get('Target', '')
            self.project.logger.debug(u'Set show workflow target as {}'.format(target))

        wf = GitWrapper.get_all_commits(target, self.settings)
        wf.build_graph(self.args.dvc_commits,
                       self.args.all_commits,
                       self.args.max_commits)
        return 0
