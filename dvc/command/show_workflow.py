import os

from dvc.command.base import CmdBase
from dvc.logger import Logger


class CmdShowWorkflow(CmdBase):
    def __init__(self, settings):
        super(CmdShowWorkflow, self).__init__(settings)

    def run(self):
        target = self.settings.parsed_args.target
        if not target:
            target = self._settings.config._config['Global'].get('Target', '')
            Logger.debug(u'Set show workflow target as {}'.format(target))

        wf = self.git.get_all_commits(target, self.settings)
        wf.build_graph(self.settings.parsed_args.dvc_commits,
                       self.settings.parsed_args.all_commits,
                       self.settings.parsed_args.max_commits)
        return 0

    @property
    def no_git_actions(self):
        return True

    @staticmethod
    def not_committed_changes_warning():
        pass
