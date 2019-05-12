import os

from dvc.scheme import Schemes
from .base import PathBASE


class PathLOCAL(PathBASE):
    scheme = Schemes.LOCAL

    def __str__(self):
        return os.path.relpath(self.path)
