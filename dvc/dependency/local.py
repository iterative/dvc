import os

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.dependency.base import DependencyBase
from dvc.dependency.base import DependencyDoesNotExistError
from dvc.dependency.base import DependencyIsNotFileOrDirError
from dvc.logger import Logger
from dvc.remote.local import RemoteLOCAL


class DependencyLOCAL(DependencyBase):
    REGEX = r'^(?P<path>(/+|.:\\+)?[^:]*)$'

    DoesNotExistError = DependencyDoesNotExistError
    IsNotFileOrDirError = DependencyIsNotFileOrDirError

    def __init__(self, stage, path, info=None, remote=None):
        self.stage = stage
        self.project = stage.project
        self.info = info
        if remote is not None:
            self.remote = remote
        else:
            self.remote = RemoteLOCAL(stage.project, {})

        if remote:
            path = os.path.join(remote.prefix, urlparse(path).path.lstrip('/'))

        if not os.path.isabs(path):
            path = self.remote.ospath(path)
            path = os.path.join(stage.cwd, path)
        self.path = os.path.abspath(os.path.normpath(path))

        self.path_info = {'scheme': 'local',
                          'path': self.path}

    def __str__(self):
        return self.rel_path

    @property
    def is_local(self):
        assert os.path.isabs(self.path)
        assert os.path.isabs(self.project.root_dir)
        return self.path.startswith(self.project.root_dir)

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

        if not os.path.isfile(self.path) and not os.path.isdir(self.path):
            raise self.IsNotFileOrDirError(self.rel_path)

        if (os.path.isfile(self.path) and os.path.getsize(self.path) == 0) or \
           (os.path.isdir(self.path) and len(os.listdir(self.path)) == 0):
            Logger.warn("File/directory '{}' is empty.".format(self.rel_path))

        self.info = self.remote.save_info(self.path_info)

    def dumpd(self):
        if self.is_local:
            path = self.remote.unixpath(os.path.relpath(self.path,
                                                        self.stage.cwd))
        else:
            path = self.path

        info = self.info.copy()
        info[self.PARAM_PATH] = path
        return info
