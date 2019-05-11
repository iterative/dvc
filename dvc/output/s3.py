from __future__ import unicode_literals

import posixpath

from dvc.path.utils import PathInfo
from dvc.remote.s3 import RemoteS3
from dvc.utils.compat import urlparse
from dvc.output.base import OutputBase


class OutputS3(OutputBase):
    REMOTE = RemoteS3

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
        super(OutputS3, self).__init__(
            stage,
            path,
            info=info,
            remote=remote,
            cache=cache,
            metric=metric,
            persist=persist,
            tags=tags,
        )
        bucket = remote.bucket if remote else urlparse(path).netloc
        path = urlparse(path).path.lstrip("/")
        if remote:
            path = posixpath.join(remote.prefix, path)

        self.path_info = PathInfo(
            self.scheme, bucket=bucket, path=path, url=self.url
        )
