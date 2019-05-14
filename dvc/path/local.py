import os

from dvc.scheme import Schemes
from .base import PathBASE


class PathLOCAL(PathBASE):
    scheme = Schemes.LOCAL

    def __str__(self):
        if os.name == "nt" and not os.path.commonprefix(
            [os.getcwd(), self.path]
        ):
            # In case of windows, when cache is on different drive than
            # workspace, we will get ValueError when trying to get relpath
            return self.path
        return os.path.relpath(self.path)
