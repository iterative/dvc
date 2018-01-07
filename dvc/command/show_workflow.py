import re
from collections import defaultdict

from dvc.logger import Logger
from dvc.command.common.base import CmdBase
from dvc.graph.workflow import Workflow
from dvc.graph.commit import Commit
from dvc.executor import Executor, ExecutorError


class CmdShowWorkflow(CmdBase):
    COMMIT_LEN = 7
    LOG_SEPARATOR = '|'
    LOG_FORMAT = ['%h', '%p', '%an', '%ai', '%s']

    def get_all_commits(self, target):
        # git log --all --abbrev=7 --pretty=format:"%h|%p|%an|%ai|%s"
        try:
            merges_map = self.get_merges_map()

            format_str = self.LOG_SEPARATOR.join(self.LOG_FORMAT)
            git_cmd = ['git', 'log', '--all', '--abbrev={}'.format(self.COMMIT_LEN),
                       '--pretty=format:{}'.format(format_str)]
            lines = Executor.exec_cmd_only_success(git_cmd).split('\n')

            branches_multimap = self.branches_multimap()

            wf = Workflow(target, merges_map, branches_multimap)
            for line in lines:
                items = line.split(self.LOG_SEPARATOR, len(self.LOG_FORMAT))
                assert len(items) == 5, 'Git wrapper: git log format has {} items, 5 expected'.format(len(items))
                hash, parent_hash, name, date, comment = items

                commit = Commit(hash, parent_hash, name, date, comment,
                                *self.was_target_changed(hash, target),
                                branch_tips=branches_multimap.get(hash))
                wf.add_commit(commit)

            return wf
        except ExecutorError:
            raise

    def was_target_changed(self, hash, target):
        git_cmd = ['git', 'show', '--pretty=', '--name-only', hash]
        changed_files = set(Executor.exec_cmd_only_success(git_cmd).split('\n'))

        if target not in changed_files:
            return False, None

        metric = utils.parse_target_metric_file(target)
        if metric is None:
            return False, None

        return True, metric

    def get_merges_map(self):
        # git log --merges --all --abbrev=7
        # {'a4b56f1': back_to_600_est}
        git_cmd = ['git', 'log', '--all', '--merges',
                   '--abbrev={}'.format(self.COMMIT_LEN)]
        # lines = map(str.strip, Executor.exec_cmd_only_success(git_cmd).split('\n'))
        # lines.map
        return {}

    def branches_multimap(self):
        git_cmd  = ['git', 'show-ref', '--abbrev={}'.format(self.COMMIT_LEN)]
        lines = Executor.exec_cmd_only_success(git_cmd).split('\n')
        items_full = map(unicode.split, lines)
        items = map(lambda it: (it[0], re.sub(r'^refs/heads/', '', it[1])), items_full)

        result = defaultdict(list)
        for (hash, branch) in items:
            result[hash].append(branch)
        return result

    def run(self):
        target = self.args.target
        if not target:
            target = self.project.config._config['Global'].get('Target', '')
            self.project.logger.debug(u'Set show workflow target as {}'.format(target))

        wf = self.get_all_commits(target)
        wf.build_graph(self.args.dvc_commits,
                       self.args.all_commits,
                       self.args.max_commits)
        return 0

