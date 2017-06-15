import os

from dvc.system import System


class Path(object):
    def __init__(self, path, git):
        if not os.path.isabs(path):
            pwd = System.get_cwd()
            path = os.path.normpath(os.path.join(pwd, path))

        self._abs = path
        self._dvc = os.path.relpath(self.abs, git.git_dir_abs)
        self._relative = os.path.relpath(self._abs, git.curr_dir_abs)
        self._filename = os.path.basename(self._abs)
        self._dirname = os.path.dirname(self._abs)

    @staticmethod
    def from_dvc_path(dvc_path, git):
        return Path(os.path.join(git.git_dir_abs, dvc_path), git)

    @property
    def dvc(self):
        return self._dvc

    @property
    def abs(self):
        return self._abs

    @property
    def relative(self):
        return self._relative

    @property
    def filename(self):
        return self._filename

    @property
    def dirname(self):
        return self._dirname