from dvc.utils.compat import urlunsplit
from dvc.scheme import Schemes
from .base import PathBASE


class PathSSH(PathBASE):
    scheme = Schemes.SSH

    def __init__(self, host, user, port, url=None, path=None):
        super(PathSSH, self).__init__(url, path)
        self.host = host
        self.user = user
        self.port = port

    def __str__(self):
        if not self.url:
            return urlunsplit(
                (
                    self.scheme,
                    "{}@{}:{}".format(self.user, self.host, self.port),
                    self.path,
                    "",
                    "",
                )
            )
        return self.url
