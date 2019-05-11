import os

from dvc.path import BasePathInfo, Schemes


class LocalPathInfo(BasePathInfo):
    scheme = Schemes.LOCAL

    def __str__(self):
        return os.path.relpath(self.path)
