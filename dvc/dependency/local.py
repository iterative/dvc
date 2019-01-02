import os

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

import dvc.logger as logger
from dvc.dependency.base import DependencyBase
from dvc.dependency.base import DependencyDoesNotExistError
from dvc.dependency.base import DependencyIsNotFileOrDirError
from dvc.remote.local import RemoteLOCAL


class DependencyLOCAL(DependencyBase):
    REGEX = r'^(?P<path>.*)$'

    DoesNotExistError = DependencyDoesNotExistError
    IsNotFileOrDirError = DependencyIsNotFileOrDirError

    def __init__(self, stage, path, info=None, remote=None):
        super(DependencyLOCAL, self).__init__(stage, path, info)
        if remote is not None:
            self.remote = remote
        else:
            self.remote = RemoteLOCAL(stage.project, {})

        if remote:
            p = os.path.join(remote.prefix,
                             urlparse(self.url).path.lstrip('/'))
        else:
            p = path

        if not os.path.isabs(p):
            p = self.remote.to_ospath(p)
            p = os.path.join(stage.cwd, p)
        p = os.path.abspath(os.path.normpath(p))

        self.path_info = {'scheme': 'local',
                          'path': p}

    def __str__(self):
        return self.rel_path

    @property
    def is_local(self):
        return (urlparse(self.url).scheme != 'remote'
                and not os.path.isabs(self.url))

    @property
    def sep(self):
        return os.sep

    @property
    def rel_path(self):
        return os.path.relpath(self.path)

    def changed(self):
        if not self.exists:
            return True

        info = self.remote.save_info(self.path_info)

        return self.info != info

    def save(self):
        if not self.exists:
            raise self.DoesNotExistError(self.rel_path)

        if not os.path.isfile(self.path) \
           and not os.path.isdir(self.path):  # pragma: no cover
            raise self.IsNotFileOrDirError(self.rel_path)

        if (os.path.isfile(self.path) and os.path.getsize(self.path) == 0) or \
           (os.path.isdir(self.path) and len(os.listdir(self.path)) == 0):
            msg = "file/directory '{}' is empty.".format(self.rel_path)
            logger.warning(msg)

        self.info = self.remote.save_info(self.path_info)

    def dumpd(self):
        if self.is_local:
            path = self.remote.unixpath(os.path.relpath(self.path,
                                                        self.stage.cwd))
        else:
            path = self.url

        info = self.info.copy()
        info[self.PARAM_PATH] = path
        return info
