import functools
import logging
import os
from typing import TYPE_CHECKING

from dvc.utils import as_posix
from dvc_objects.fs.base import FileSystem

if TYPE_CHECKING:
    from dvc_data.fs import DataFileSystem as _DataFileSystem


logger = logging.getLogger(__name__)


class DataFileSystem(FileSystem):
    protocol = "local"

    PARAM_CHECKSUM = "md5"

    def _prepare_credentials(self, **config):
        return config

    @functools.cached_property
    def fs(  # pylint: disable=invalid-overridden-method
        self,
    ) -> "_DataFileSystem":
        from dvc_data.fs import DataFileSystem as _DataFileSystem

        return _DataFileSystem(**self.fs_args)

    def isdvc(self, path, **kwargs):
        return self.fs.isdvc(path, **kwargs)

    def from_os_path(self, path):
        if os.path.isabs(path):
            path = os.path.splitdrive(path)[1]

        return as_posix(path)
