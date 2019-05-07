from dvc.utils.compat import urlunsplit
from dvc.path import BasePathInfo, Schemes


class HDFSPathInfo(BasePathInfo):
    scheme = Schemes.HDFS

    def __init__(self, user, url=None, path=None):
        super(HDFSPathInfo, self).__init__(url, path)
        self.user = user

    def __str__(self):
        if not self.url:
            return urlunsplit((self.scheme, self.user, self.path, "", ""))
        return self.url
