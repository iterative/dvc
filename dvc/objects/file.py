import errno
import logging
import os
from typing import TYPE_CHECKING, Optional

from .errors import ObjectFormatError

if TYPE_CHECKING:
    from dvc.fs.base import BaseFileSystem
    from dvc.hash_info import HashInfo
    from dvc.types import AnyPath

    from .db.base import ObjectDB

logger = logging.getLogger(__name__)


class HashFile:
    def __init__(
        self,
        path_info: Optional["AnyPath"],
        fs: Optional["BaseFileSystem"],
        hash_info: "HashInfo",
        name: Optional[str] = None,
    ):
        self.path_info = path_info
        self.fs = fs
        self.hash_info = hash_info
        self.name = name

    def __len__(self):
        return 1

    def __str__(self):
        return f"object {self.hash_info}"

    def __bool__(self):
        return bool(self.hash_info)

    def __eq__(self, other):
        if not isinstance(other, HashFile):
            return False
        return (
            self.path_info == other.path_info
            and self.fs == other.fs
            and self.hash_info == other.hash_info
        )

    def __hash__(self):
        return hash(
            (
                self.hash_info,
                self.path_info,
                self.fs.scheme if self.fs else None,
            )
        )

    def check(self, odb: "ObjectDB", check_hash: bool = True):
        if not check_hash:
            assert self.fs
            if not self.fs.exists(self.path_info):
                raise FileNotFoundError(
                    errno.ENOENT, os.strerror(errno.ENOENT), self.path_info
                )
            else:
                return None

        self._check_hash(odb)

    def _check_hash(self, odb):
        from .stage import get_file_hash

        _, actual = get_file_hash(
            self.path_info, self.fs, self.hash_info.name, odb.state
        )

        logger.trace(
            "cache '%s' expected '%s' actual '%s'",
            self.path_info,
            self.hash_info,
            actual,
        )

        assert actual.name == self.hash_info.name
        if actual.value.split(".")[0] != self.hash_info.value.split(".")[0]:
            raise ObjectFormatError(f"{self} is corrupted")
