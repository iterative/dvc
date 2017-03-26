import os


class Path(object):
    def __init__(self, relative_raw, git):
        self._abs = os.path.abspath(relative_raw)
        self._dvc = os.path.relpath(self.abs, git.git_dir_abs)
        self._relative = os.path.relpath(self._abs, git.curr_dir_abs)
        self._filename = os.path.basename(self._abs)

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
