import posixpath

from dvc.utils.compat import urlparse
from dvc.output.base import OutputBase
from dvc.remote.ssh import RemoteSSH


class OutputSSH(OutputBase):
    REMOTE = RemoteSSH

    def __init__(self,
                 stage,
                 path,
                 info=None,
                 remote=None,
                 cache=True,
                 metric=False):
        super(OutputSSH, self).__init__(stage,
                                        path,
                                        info=info,
                                        remote=remote,
                                        cache=cache,
                                        metric=metric)
        host = remote.host if remote else self.group('host')
        port = remote.port if remote else RemoteSSH.DEFAULT_PORT
        user = remote.user if remote else self.group('user')

        if remote:
            path = posixpath.join(remote.prefix,
                                  urlparse(path).path.lstrip('/'))
        else:
            path = self.match(self.url).group('path')

        self.path_info = {'scheme': 'ssh',
                          'host': host,
                          'port': port,
                          'user': user,
                          'path': path}
