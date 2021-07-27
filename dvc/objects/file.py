import errno
import logging
import os
import pickle
from typing import TYPE_CHECKING, Optional, Tuple

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

    @property
    def size(self):
        return self.hash_info.size

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

        actual = get_file_hash(
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


class ReferenceHashFile(HashFile):
    PARAM_PATH = "path"
    PARAM_HASH = "hash"
    PARAM_MTIME = "mtime"
    PARAM_SIZE = "size"
    PARAM_FS_CONFIG = "fs_config"

    def __init__(
        self,
        path_info: "AnyPath",
        fs: "BaseFileSystem",
        hash_info: "HashInfo",
        mtime: Optional[int] = None,
        size: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(path_info, fs, hash_info, **kwargs)
        cur_mtime, cur_size = self._get_mtime_and_size()
        self.mtime = cur_mtime if mtime is None else mtime
        self.hash_info.size = cur_size if size is None else size

    def __str__(self):
        return f"ref object {self.hash_info} -> {self.path_info}"

    def check(self, odb: "ObjectDB", check_hash: bool = True):
        if not check_hash:
            assert self.fs
            if not self.fs.exists(self.path_info):
                raise FileNotFoundError(
                    errno.ENOENT, os.strerror(errno.ENOENT), self.path_info
                )
            if not (self.mtime, self.size) == self._get_mtime_and_size():
                raise ObjectFormatError(f"{self} is changed")
            return
        self._check_hash(odb)

    def _get_mtime_and_size(self) -> Tuple[Optional[int], int]:
        from dvc.utils.fs import get_mtime_and_size

        assert self.fs
        if hasattr(self.fs, "stat"):
            try:
                return get_mtime_and_size(self.path_info, self.fs)
            except FileNotFoundError:
                pass
        return None, self.fs.getsize(self.path_info)

    def to_bytes(self):
        # NOTE: dumping reference FS's this way is insecure, as the
        # fully parsed remote FS config will include credentials
        #
        # ReferenceHashFiles should currently only be serialized in
        # memory and not to disk
        dict_ = {
            self.PARAM_PATH: self.path_info,
            self.PARAM_HASH: self.hash_info,
            self.PARAM_MTIME: self.mtime,
            self.PARAM_SIZE: self.size,
            self.PARAM_FS_CONFIG: self.fs.config,
        }
        try:
            return pickle.dumps(dict_)
        except pickle.PickleError as exc:
            raise ObjectFormatError(f"Could not pickle {self}") from exc

    @classmethod
    def from_bytes(cls, data):
        from dvc.external_repo import external_repo
        from dvc.fs import get_fs_cls
        from dvc.fs.repo import RepoFileSystem

        try:
            dict_ = pickle.loads(data)
        except pickle.PickleError as exc:
            raise ObjectFormatError("ReferenceHashFile is corrupted") from exc

        try:
            path_info = dict_[cls.PARAM_PATH]
            hash_info = dict_[cls.PARAM_HASH]
        except KeyError as exc:
            raise ObjectFormatError("ReferenceHashFile is corrupted") from exc

        config = dict_.get(cls.PARAM_FS_CONFIG, {})
        if RepoFileSystem.PARAM_REPO_URL in config:
            with external_repo(
                config[RepoFileSystem.PARAM_REPO_URL],
                rev=config.get(RepoFileSystem.PARAM_REV),
            ) as repo:
                fs = repo.repo_fs
        else:
            fs_cls = get_fs_cls(config, scheme=path_info.scheme)
            fs = fs_cls(**config)
        return ReferenceHashFile(
            path_info,
            fs,
            hash_info,
            mtime=dict_.get(cls.PARAM_MTIME),
            size=dict_.get(cls.PARAM_SIZE),
        )
