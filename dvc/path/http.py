from dvc.path import BasePathInfo, Schemes
from dvc.utils.compat import urlparse, urlunsplit


class HTTPPathInfo(BasePathInfo):
    @property
    def scheme(self):
        if self.path:
            return urlparse(self.path).scheme
        else:
            return Schemes.HTTP

    def __str__(self):
        if not self.url:
            return urlunsplit((self.scheme, self.path, "", "", ""))
        return self.url
