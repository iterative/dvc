import errno
import logging
import os
import pickle
from typing import TYPE_CHECKING, Optional

from dvc_objects.errors import ObjectFormatError
from dvc_objects.file import HashFile
from dvc_objects.fs import FS_MAP, LocalFileSystem

if TYPE_CHECKING:
    from dvc_objects.db import ObjectDB
    from dvc_objects.fs.base import AnyFSPath, FileSystem
    from dvc_objects.hash_info import HashInfo

logger = logging.getLogger(__name__)


class ReferenceHashFile(HashFile):
    PARAM_PATH = "path"
    PARAM_HASH = "hash"
    PARAM_CHECKSUM = "checksum"
    PARAM_FS_CONFIG = "fs_config"
    PARAM_FS_CLS = "fs_name"

    def __init__(
        self,
        fs_path: "AnyFSPath",
        fs: "FileSystem",
        hash_info: "HashInfo",
        checksum: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(fs_path, fs, hash_info, **kwargs)
        self.checksum = checksum or fs.checksum(fs_path)

    def __str__(self):
        return f"ref object {self.hash_info} -> {self.fs_path}"

    def check(self, odb: "ObjectDB", check_hash: bool = True):
        assert self.fs
        if not self.fs.exists(self.fs_path):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), self.fs_path
            )
        if self.checksum != self._get_checksum():
            raise ObjectFormatError(f"{self} is changed")
        if check_hash:
            self._check_hash(odb)

    def _get_checksum(self) -> str:
        assert self.fs
        return self.fs.checksum(self.fs_path)

    def to_bytes(self):
        # NOTE: dumping reference FS's this way is insecure, as the
        # fully parsed remote FS config will include credentials
        #
        # ReferenceHashFiles should currently only be serialized in
        # memory and not to disk
        fs_path = self.fs_path
        fs_cls = type(self.fs)
        mod = None
        if fs_cls not in FS_MAP.values() and fs_cls != LocalFileSystem:
            mod = ".".join((fs_cls.__module__, fs_cls.__name__))
        dict_ = {
            self.PARAM_PATH: fs_path,
            self.PARAM_HASH: self.hash_info,
            self.PARAM_CHECKSUM: self.checksum,
            self.PARAM_FS_CONFIG: self.config_tuple(self.fs),
            self.PARAM_FS_CLS: mod,
        }
        try:
            return pickle.dumps(dict_)
        except pickle.PickleError as exc:
            raise ObjectFormatError(f"Could not pickle {self}") from exc

    @classmethod
    def from_bytes(cls, data: bytes, fs_cache: Optional[dict] = None):
        from dvc_objects.fs import get_fs_cls

        try:
            dict_ = pickle.loads(data)
        except pickle.PickleError as exc:
            raise ObjectFormatError("ReferenceHashFile is corrupted") from exc

        try:
            fs_path = dict_[cls.PARAM_PATH]
            hash_info = dict_[cls.PARAM_HASH]
        except KeyError as exc:
            raise ObjectFormatError("ReferenceHashFile is corrupted") from exc

        protocol, config_pairs = dict_.get(cls.PARAM_FS_CONFIG)
        fs = fs_cache.get((protocol, config_pairs)) if fs_cache else None
        if not fs:
            config = dict(config_pairs)
            mod = dict_.get(cls.PARAM_FS_CLS, None)
            fs_cls = get_fs_cls(config, cls=mod, scheme=protocol)
            fs = fs_cls(**config)
        return ReferenceHashFile(
            fs_path,
            fs,
            hash_info,
            checksum=dict_.get(cls.PARAM_CHECKSUM),
        )

    @staticmethod
    def config_tuple(fs: "FileSystem"):
        return (
            fs.protocol,
            tuple(
                (key, value)
                for key, value in sorted(
                    fs.config.items(), key=lambda item: item[0]
                )
            ),
        )
