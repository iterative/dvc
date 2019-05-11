from __future__ import unicode_literals

import getpass
import posixpath

from dvc.path.ssh import SSHPathInfo
from dvc.utils.compat import urlparse
from dvc.output.base import OutputBase
from dvc.remote.ssh import RemoteSSH


class OutputSSH(OutputBase):
    REMOTE = RemoteSSH

    def __init__(
        self,
        stage,
        path,
        info=None,
        remote=None,
        cache=True,
        metric=False,
        persist=False,
        tags=None,
    ):
        super(OutputSSH, self).__init__(
            stage,
            path,
            info=info,
            remote=remote,
            cache=cache,
            metric=metric,
            persist=persist,
            tags=tags,
        )
        parsed = urlparse(path)
        host = remote.host if remote else parsed.hostname
        port = (
            remote.port if remote else (parsed.port or RemoteSSH.DEFAULT_PORT)
        )
        user = (
            remote.user if remote else (parsed.username or getpass.getuser())
        )

        if remote:
            path = posixpath.join(
                remote.prefix, urlparse(path).path.lstrip("/")
            )
        else:
            path = parsed.path

        self.path_info = SSHPathInfo(
            host=host, user=user, port=port, url=self.url, path=path
        )
