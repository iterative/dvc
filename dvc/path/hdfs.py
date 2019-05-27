from dvc.scheme import Schemes
from .base import PathBASE


class PathHDFS(PathBASE):
    scheme = Schemes.HDFS

    def __init__(self, user, url=None, path=None):
        super(PathHDFS, self).__init__(url, path)
        self.user = user

    def __str__(self):
        return self.path
