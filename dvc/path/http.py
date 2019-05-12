from dvc.scheme import Schemes
from dvc.utils.compat import urlunsplit

from .base import PathBASE


class PathHTTP(PathBASE):
    scheme = Schemes.HTTP

    def __str__(self):
        if not self.url:
            return urlunsplit((self.scheme, self.path, "", "", ""))
        return self.url
