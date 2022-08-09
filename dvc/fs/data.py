import logging
import os
import threading

from funcy import cached_property, wrap_prop

from dvc.utils import as_posix
from dvc_data.fs import DataFileSystem as _DataFileSystem
from dvc_objects.fs.base import FileSystem

logger = logging.getLogger(__name__)


class DataFileSystem(FileSystem):
    protocol = "local"

    PARAM_CHECKSUM = "md5"

    def _prepare_credentials(self, **config):
        return config

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        return _DataFileSystem(**self.fs_args)

    def isdvc(self, path, **kwargs):
        return self.fs.isdvc(path, **kwargs)

    def from_os_path(self, path):
        if os.path.isabs(path):
            path = os.path.splitdrive(path)[1]

        return as_posix(path)
