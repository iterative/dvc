from __future__ import unicode_literals

import os
import logging

from dvc.path.local import LocalPathInfo
from dvc.utils.compat import urlparse
from dvc.istextfile import istextfile
from dvc.exceptions import DvcException
from dvc.remote.local import RemoteLOCAL
from dvc.output.base import OutputBase


logger = logging.getLogger(__name__)


class OutputLOCAL(OutputBase):
    REMOTE = RemoteLOCAL

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
        super(OutputLOCAL, self).__init__(
            stage,
            path,
            info,
            remote=remote,
            cache=cache,
            metric=metric,
            persist=persist,
            tags=tags,
        )
        if remote:
            p = os.path.join(
                remote.prefix, urlparse(self.url).path.lstrip("/")
            )
        else:
            p = path

        if not os.path.isabs(p):
            p = self.remote.to_ospath(p)
            p = os.path.join(stage.wdir, p)
        p = os.path.abspath(os.path.normpath(p))

        self.path_info = LocalPathInfo(url=self.url, path=p)

    def __str__(self):
        return self.rel_path

    @property
    def is_in_repo(self):
        return urlparse(self.url).scheme != "remote" and not os.path.isabs(
            self.url
        )

    def assign_to_stage_file(self, stage):
        from dvc.repo import Repo

        fullpath = os.path.abspath(stage.wdir)
        self.path_info.path = os.path.join(fullpath, self.stage_path)

        self.repo = Repo(self.path)

        self.stage = stage
        return self

    @property
    def sep(self):
        return os.sep

    @property
    def rel_path(self):
        return os.path.relpath(self.path)

    @property
    def stage_path(self):
        return os.path.relpath(self.path, self.stage.wdir)

    def dumpd(self):
        ret = super(OutputLOCAL, self).dumpd()
        if self.is_in_repo:
            path = self.remote.to_posixpath(
                os.path.relpath(self.path, self.stage.wdir)
            )
        else:
            path = self.url

        ret[self.PARAM_PATH] = path

        return ret

    def verify_metric(self):
        if not self.metric:
            return

        if not os.path.exists(self.path):
            return

        if os.path.isdir(self.path):
            msg = "directory '{}' cannot be used as metrics."
            raise DvcException(msg.format(self.rel_path))

        if not istextfile(self.path):
            msg = "binary file '{}' cannot be used as metrics."
            raise DvcException(msg.format(self.rel_path))
