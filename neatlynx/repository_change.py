import os

from neatlynx.data_file_obj import DataFileObj, NotInDataDirError
from neatlynx.exceptions import NeatLynxException
from neatlynx.git_wrapper import GitWrapper


class RepositoryChangeError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'Repository change error: {}'.format(msg))


class FileModificationState(object):
    STATUS_UNTRACKED = '?'
    STATUS_DELETE = 'D'
    STATUS_MODIFIED = 'M'
    STATUS_TYPE_CHANGED = 'T'

    def __init__(self, state, file):
        self.state = state
        self.file = file

    def _check_status(self, status):
        return self.state.find(status) >= 0

    @property
    def is_removed(self):
        return self._check_status(self.STATUS_DELETE)

    @property
    def is_modified(self):
        return self._check_status(self.STATUS_MODIFIED) \
               or self._check_status(self.STATUS_TYPE_CHANGED)

    @property
    def is_new(self):
        return self._check_status(self.STATUS_UNTRACKED)

    @property
    def is_unusual(self):
        return self.is_new or self.is_modified or self.is_removed


class RepositoryChange(object):
    """Pre-condition: git repository has no changes"""

    def __init__(self, args, stdout, stderr, git, config):
        self.git = git
        self.config = config

        GitWrapper.exec_cmd_only_success(args, stdout, stderr)
        self._file_states = self._get_file_states()

        self._changed_dobj, self._externally_created_files = DataFileObj.files_to_dobjs(
            self.changed_files, self.git, self.config)

    @staticmethod
    def exec_cmd(args, stdout, stderr, git):
        return RepositoryChange(args, stdout, stderr, git)

    @staticmethod
    def get_filenames(mod_state_list):
        return list(map(lambda x: x.file, mod_state_list))

    @property
    def removed_files(self):
        return self.get_filenames(filter(lambda x: x.is_removed, self._file_states))

    @property
    def modified_files(self):
        return self.get_filenames(filter(lambda x: x.is_modified, self._file_states))

    @property
    def new_files(self):
        return self.get_filenames(filter(lambda x: x.is_new, self._file_states))

    @property
    def changed_files(self):
        return self.new_files + self.modified_files

    @property
    def unusual_state_files(self):
        return self.get_filenames(filter(lambda x: not x.is_unusual, self._file_states))

    def _get_file_states(self):
        statuses = GitWrapper.status_files()

        result = []
        for status, file in statuses:
            file_path = os.path.join(self.git.git_dir_abs, file)
            if os.path.isfile(file_path):
                result.append(FileModificationState(status, file_path))
            else:
                files = []
                self.get_all_files_from_dir(file_path, files)
                state = FileModificationState.STATUS_UNTRACKED + FileModificationState.STATUS_UNTRACKED
                for f in files:
                    result.append(FileModificationState(state, f))

        return result

    def get_all_files_from_dir(self, dir, result):
        if not os.path.isdir(dir):
            raise RepositoryChangeError('Changed path {} is not directory'.format(dir))

        files = os.listdir(dir)
        for f in files:
            path = os.path.join(dir, f)
            if os.path.isfile(path):
                result.append(path)
            else:
                self.get_all_files_from_dir(path, result)
        pass

    @property
    def dobj_for_changed_files(self):
        return self._changed_dobj

    @property
    def externally_created_files(self):
        return self._externally_created_files
