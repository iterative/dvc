import os
import re
from collections import defaultdict

from dvc.exceptions import DvcException
from dvc.logger import Logger
from dvc.config import Config
from dvc.executor import Executor, ExecutorError
from dvc.path.data_item import DataItemError
from dvc.state_file import StateFile
from dvc.system import System
from dvc.graph.workflow import Workflow
from dvc.graph.commit import Commit


class GitWrapperError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, msg)


class GitWrapperI(object):
    COMMIT_LEN = 7

    def __init__(self, git_dir=None, commit=None):
        self._git_dir = git_dir
        self._commit = commit

    @property
    def git_dir(self):
        return self._git_dir

    @property
    def lock_file(self):
        return os.path.join(self.git_dir_abs, Config.CONFIG_DIR, '.' + Config.CONFIG + '.lock')

    @property
    def git_dir_abs(self):
        return System.realpath(self.git_dir)

    @property
    def curr_dir_abs(self):
        return os.path.abspath(os.curdir)

    @property
    def curr_commit(self):
        return self._commit

    def is_ready_to_go(self):
        return True

    @staticmethod
    def git_file_statuses():
        Logger.debug('[dvc-git] Getting file statuses. Command: git status --porcelain')
        code, out, err = Executor.exec_cmd(['git', 'status', '--porcelain'])
        if code != 0:
            raise ExecutorError('[dvc-git] File status command error - {}'.format(err))
        Logger.debug('[dvc-git] Getting file statuses. Success.')

        return GitWrapper.parse_porcelain_files(out)

    @staticmethod
    def git_config_get(name):
        code, out, err = Executor.exec_cmd(['git', 'config', '--get', name])
        Logger.debug('[dvc-git] "git config --get {}": code({}), out({}), err({})'.format(name,
                                                                                          code,
                                                                                          out,
                                                                                          err))
        if code != 0:
            return None
        return out

    @staticmethod
    def git_path_to_system_path(path):
        if os.name == 'nt':
            return path.replace('/', '\\')
        return path

    @staticmethod
    def parse_porcelain_files(out):
        result = []
        if len(out) > 0:
            lines = out.split('\n')
            for line in lines:
                status = line[:2]
                file = GitWrapperI.git_path_to_system_path(line[3:])
                result.append((status, file))
        return result

    def abs_paths_to_dvc(self, files):
        result = []
        for file in files:
            result.append(os.path.relpath(os.path.abspath(file), self.git_dir_abs))

        return result

    def commit_all_changes_and_log_status(self, message):
        pass

    @staticmethod
    def modify_gitignore(flist):
        for fname in flist:
            # We're modifying .gitignore in the same dir as the file itself, so
            # that if dir consists only of data files it will be still tracked by
            # git and we will not have any problems with --porcelain output for
            # specific changed files.
            gitignore = os.path.join(os.path.dirname(fname), '.gitignore')
            with open(gitignore, 'a') as fd:
                fd.write('\n' + os.path.basename(fname))


class GitWrapper(GitWrapperI):
    FLOATS_FROM_STRING = re.compile(r'[-+]?(?:(?:\d*\.\d+)|(?:\d+\.?))(?:[Ee][+-]?\d+)?')

    def __init__(self):
        super(GitWrapper, self).__init__()

    def is_inside_work_tree(self):
        code, out, err = Executor.exec_cmd(['git', 'rev-parse', '--is-inside-work-tree'])
        if code != 0:
            Logger.error('[dvc-git] Not inside git tree');
        return code == 0

    def is_ready_to_go(self):
        if not self.is_inside_work_tree():
            return False

        statuses = self.git_file_statuses()
        if len(statuses) > 0:
            Logger.error('[dvc-git] Commit all changed files before running '
                         'reproducible command. Changed files:')
            for status, file in statuses:
                Logger.error("{} {}".format(status, file))
            return False

        return self.check_config()

    def check_config(self):
        # Sanity check to make sure we will be able to commit
        name = self.git_config_get('user.name')
        if name == None:
            Logger.error('[dvc-git] Please setup user.name in git config')
            return False

        email = self.git_config_get('user.email')
        if email == None:
            Logger.error('[dvc-git] Please setup user.email in git config')
            return False

        return True

    @property
    def curr_dir_dvc(self):
        return os.path.relpath(os.path.abspath(os.curdir), self.git_dir_abs)

    @property
    def git_dir(self):
        if self._git_dir:
            return self._git_dir

        try:
            Logger.debug('[dvc-git] Getting git directory. Command: git rev-parse --show-toplevel')
            code, out, err = Executor.exec_cmd(['git', 'rev-parse', '--show-toplevel'])

            if code != 0:
                raise ExecutorError('[dvc-git] Git directory command error - {}'.format(err))
            Logger.debug('[dvc-git] Getting git directory. Success.')

            self._git_dir = out
            return self._git_dir
        except ExecutorError:
            raise
        except Exception as e:
            raise ExecutorError('Unable to run git command: {}'.format(e))
        pass

    @property
    def curr_commit(self):
        Logger.debug('[dvc-git] Getting current git commit. Command: git rev-parse --short HEAD')

        code, out, err = Executor.exec_cmd(['git', 'rev-parse', 'HEAD'])
        if code != 0:
            raise ExecutorError('[dvc-git] Commit command error - {}'.format(err))
        Logger.debug('[dvc-git] Getting current git commit. Success.')
        return out

    @staticmethod
    def commit_all_changes(message):
        Logger.debug('[dvc-git] Commit all changes. Commands: {} && {} && {}'.format(
            'git add --all', 'git status --porcelain', 'git commit -m'))

        Executor.exec_cmd_only_success(['git', 'add', '--all'])
        out_status = Executor.exec_cmd_only_success(['git', 'status', '--porcelain'])
        changes = GitWrapper.parse_porcelain_files(out_status)
        if len(changes) == 0:
            return []

        Executor.exec_cmd_only_success(['git', 'commit', '-m', '\'{}\''.format(message)])
        Logger.debug('[dvc-git] Commit all changes. Success.')
        return changes

    def commit_all_changes_and_log_status(self, message):
        statuses = self.commit_all_changes(message)
        Logger.debug('[dvc-git] A new commit {} was made in the current branch. '
                     'Added files:'.format(self.curr_commit))
        for status, file in statuses:
            Logger.debug('[dvc-git]\t{} {}'.format(status, file))
        pass

    @staticmethod
    def abs_paths_to_relative(files):
        cur_dir = System.realpath(os.curdir)

        result = []
        for file in files:
            result.append(os.path.relpath(System.realpath(file), cur_dir))

        return result

    def dvc_paths_to_abs(self, files):
        results = []

        for file in files:
            results.append(os.path.join(self.git_dir_abs, file))

        return results

    def were_files_changed(self, code_dependencies, path_factory, changed_files):
        code_files, code_dirs = self.separate_dependency_files_and_dirs(code_dependencies)
        code_files_set = set([path_factory.path(x).dvc for x in code_files])
        for changed_file in changed_files:
            if changed_file in code_files_set:
                return True

            for dir in code_dirs:
                if changed_file.startswith(dir):
                    return True

        return False

    @staticmethod
    def get_changed_files(target_commit):
        Logger.debug('[dvc-git] Identify changes. Command: '
                     'git diff --name-only HEAD {}'.format(target_commit))

        changed_files_str = Executor.exec_cmd_only_success(['git',
                                                            'diff',
                                                            '--name-only',
                                                            'HEAD',
                                                            target_commit])
        changed_files = changed_files_str.strip('"').split('\n')

        Logger.debug('[dvc-git] Identify changes. Success. Changed files: {}'.format(
            ', '.join(changed_files)))
        return changed_files

    @staticmethod
    def get_target_commit(fname):
        Logger.debug('[dvc-git] Get target commit. Command: '
                     'git log -1 --pretty=format:"%h" {}'.format(fname))
        try:
            commit = Executor.exec_cmd_only_success(['git',
                                                     'log',
                                                     '-1',
                                                     '--pretty=format:"%h"',
                                                     fname])
            return commit.strip('"')
        except ExecutorError:
            return None

    @staticmethod
    def get_last_merge_changed_files():
        cmd = 'git diff --name-only @{1}'
        Logger.debug('[dvc-git] Get last merge changed files. Command: {}'.format(cmd))
        try:
            fstr = Executor.exec_cmd_only_success(cmd.split())
            return fstr.split('\n')
        except ExecutorError:
            return None

    @staticmethod
    def checkout_file_before_last_merge(fname):
        cmd = 'git checkout @{1} -- ' + fname
        Logger.debug('[dvc-git] Checkout file {} before last merge. Command: {}'.format(fname, cmd))
        Executor.exec_cmd_only_success(cmd.split())

    def separate_dependency_files_and_dirs(self, code_dependencies):
        code_files = []
        code_dirs = []

        code_dependencies_abs = self.dvc_paths_to_abs(code_dependencies)
        for code in code_dependencies_abs:
            if os.path.isdir(code):
                code_dirs.append(code)
            else:
                code_files.append(code)

        return code_files, code_dirs

    LOG_SEPARATOR = '|'
    LOG_FORMAT = ['%h', '%p', '%an', '%ai', '%s']

    def get_all_commits(self, target, settings):
        # git log --all --abbrev=7 --pretty=format:"%h|%p|%an|%ai|%s"
        try:
            merges_map = GitWrapper.get_merges_map()

            format_str = GitWrapper.LOG_SEPARATOR.join(GitWrapper.LOG_FORMAT)
            git_cmd = ['git', 'log', '--all', '--abbrev={}'.format(GitWrapper.COMMIT_LEN),
                       '--pretty=format:{}'.format(format_str)]
            lines = Executor.exec_cmd_only_success(git_cmd).split('\n')

            branches_multimap = GitWrapper.branches_multimap()

            wf = Workflow(target, merges_map, branches_multimap)
            for line in lines:
                items = line.split(GitWrapper.LOG_SEPARATOR, len(GitWrapper.LOG_FORMAT))
                assert len(items) == 5, 'Git wrapper: git log format has {} items, 5 expected'.format(len(items))
                hash, parent_hash, name, date, comment = items

                commit = Commit(hash, parent_hash, name, date, comment,
                                *self.was_target_changed(hash, target, settings),
                                branch_tips=branches_multimap.get(hash))
                wf.add_commit(commit)

            return wf
        except ExecutorError:
            raise

    def was_target_changed(self, hash, target, settings):
        git_cmd = ['git', 'show', '--pretty=', '--name-only', hash]
        changed_files = set(Executor.exec_cmd_only_success(git_cmd).split('\n'))

        if target not in changed_files:
            return False, None

        metric = self._read_metric_from_state_file(hash, target, settings)
        if metric is None:
            return False, None

        return True, metric

    def _read_metric_from_state_file(self, hash, target, settings):
        try:
            data_item = settings.path_factory.data_item(target)
        except DataItemError as ex:
            Logger.warn('Target file {} is not data item: {}'.format(target, ex))
            return None

        try:
            cmd_corresponded_state_file = ['git', 'show', '{}:{}'.format(hash, data_item.state.relative)]
            state_file_content = Executor.exec_cmd_only_success(cmd_corresponded_state_file)
        except ExecutorError as ex:
            msg = '[dvc-git] Cannot obtain content of target symbolic file {} with hash {}: {}'
            Logger.warn(msg.format(target, hash, ex))
            return None

        state_file = StateFile.loads(state_file_content, settings)
        return state_file.single_target_metric

    @staticmethod
    def get_merges_map():
        # git log --merges --all --abbrev=7
        # {'a4b56f1': back_to_600_est}
        git_cmd = ['git', 'log', '--all', '--merges',
                   '--abbrev={}'.format(GitWrapper.COMMIT_LEN)]
        # lines = map(str.strip, Executor.exec_cmd_only_success(git_cmd).split('\n'))
        # lines.map
        return {}

    @staticmethod
    def branches_multimap():
        git_cmd  = ['git', 'show-ref', '--abbrev={}'.format(GitWrapper.COMMIT_LEN)]
        lines = Executor.exec_cmd_only_success(git_cmd).split('\n')
        items_full = map(unicode.split, lines)
        items = map(lambda it: (it[0], re.sub(r'^refs/heads/', '', it[1])), items_full)

        result = defaultdict(list)
        for (hash, branch) in items:
            result[hash].append(branch)
        return result
