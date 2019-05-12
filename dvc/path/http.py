from dvc.path import BasePathInfo, Schemes
from dvc.utils.compat import urlunsplit


class HTTPPathInfo(BasePathInfo):
    scheme = Schemes.HTTP

    def __str__(self):
        if not self.url:
            return urlunsplit((self.scheme, self.path, "", "", ""))
        return self.url
