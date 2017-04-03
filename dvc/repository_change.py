import os

from dvc.exceptions import DvcException
from dvc.git_wrapper import GitWrapper
from dvc.executor import Executor
from dvc.logger import Logger
from dvc.path.data_item import NotInDataDirError
from dvc.path.stated_data_item import StatedDataItem
from dvc.utils import cached_property


class RepositoryChangeError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Repository change error: {}'.format(msg))


class RepositoryChange(object):
    """Pre-condition: git repository has no changes"""

    def __init__(self, args, stdout, stderr, git, config, path_factory):
        self.git = git
        self.config = config
        self.path_factory = path_factory

        Logger.debug(u'[Repository change] Exec command: {}. stdout={}, stderr={}'.format(
                     u' '.join(args),
                     stdout,
                     stderr))
        Executor.exec_cmd_only_success(args, stdout, stderr)

        self._stated_data_items = []
        self._externally_created_files = []
        self._init_file_states()

    @staticmethod
    def exec_cmd(args, stdout, stderr, git, path_factory):
        return RepositoryChange(args, stdout, stderr, git)

    @cached_property
    def removed_data_items(self):
        return [x for x in self._stated_data_items if x.is_removed]

    @cached_property
    def modified_data_items(self):
        return [x for x in self._stated_data_items if x.is_modified]

    @cached_property
    def new_data_items(self):
        return [x for x in self._stated_data_items if x.is_new]

    @cached_property
    def unusual_data_items(self):
        return [x for x in self._stated_data_items if x.is_unusual]

    @property
    def changed_data_items(self):
        return self.new_data_items + self.modified_data_items

    def _add_stated_data_item(self, state, file):
        try:
            item = self.path_factory.stated_data_item(state, file)
            self._stated_data_items.append(item)
            Logger.debug('[Repository change] Add status: {} {}'.format(
                         item.status,
                         item.data.dvc))
        except NotInDataDirError:
            self._externally_created_files.append(file)
        pass

    def _init_file_states(self):
        statuses = GitWrapper.git_file_statuses()

        for status, file in statuses:
            file_path = os.path.join(self.git.git_dir_abs, file)

            if not os.path.isdir(file_path):
                self._add_stated_data_item(status, file_path)
            else:
                files = []
                self.get_all_files_from_dir(file_path, files)
                state = StatedDataItem.STATUS_UNTRACKED + StatedDataItem.STATUS_UNTRACKED
                for f in files:
                    self._add_stated_data_item(state, f)
        pass

    def get_all_files_from_dir(self, dir, result):
        files = os.listdir(dir)
        for f in files:
            path = os.path.join(dir, f)
            if os.path.isfile(path):
                result.append(path)
            else:
                self.get_all_files_from_dir(path, result)
        pass

    @property
    def externally_created_files(self):
        return self._externally_created_files
