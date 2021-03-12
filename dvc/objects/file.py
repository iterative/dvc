import logging

from .errors import ObjectFormatError
from .stage import get_file_hash

logger = logging.getLogger(__name__)


class HashFile:
    def __init__(self, path_info, fs, hash_info):
        self.path_info = path_info
        self.fs = fs
        self.hash_info = hash_info

    @property
    def size(self):
        if not (self.path_info and self.fs):
            return None
        return self.fs.getsize(self.path_info)

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

    def check(self, odb):
        actual = get_file_hash(
            self.path_info, self.fs, self.hash_info.name, odb.repo.state
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
