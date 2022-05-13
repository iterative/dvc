import os
import pickle
from functools import wraps
from typing import Any

import diskcache
from diskcache import Disk as disk
from diskcache import Index  # noqa: F401, pylint: disable=unused-import
from diskcache import Timeout  # noqa: F401, pylint: disable=unused-import

# pylint: disable=redefined-builtin


class DiskError(Exception):
    def __init__(self, directory: str, type: str) -> None:
        self.directory = directory
        self.type = type
        super().__init__(f"Could not open disk '{type}' in {directory}")


def translate_pickle_error(fn):
    @wraps(fn)
    def wrapped(self, *args, **kwargs):
        try:
            return fn(self, *args, **kwargs)
        except (pickle.PickleError, ValueError) as e:
            if isinstance(e, ValueError) and "pickle protocol" not in str(e):
                raise
            # pylint: disable=protected-access
            raise DiskError(self._directory, type=self._type) from e

    return wrapped


class Disk(disk):
    """Reraise pickle-related errors as DiskError."""

    # we need type to differentiate cache for better error messages
    _type: str

    put = translate_pickle_error(disk.put)
    get = translate_pickle_error(disk.get)
    store = translate_pickle_error(disk.store)
    fetch = translate_pickle_error(disk.fetch)


class Cache(diskcache.Cache):
    """Extended to handle pickle errors and use a constant pickle protocol."""

    def __init__(
        self,
        directory: str = None,
        timeout: int = 60,
        disk: disk = Disk,  # pylint: disable=redefined-outer-name
        type: str = None,
        **settings: Any,
    ) -> None:
        settings.setdefault("disk_pickle_protocol", 4)
        super().__init__(
            directory=directory, timeout=timeout, disk=disk, **settings
        )
        self.disk._type = self._type = type or os.path.basename(self.directory)

    def __getstate__(self):
        return (*super().__getstate__(), self._type)
