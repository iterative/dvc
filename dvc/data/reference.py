import errno
import logging
import os
import pickle
from typing import TYPE_CHECKING, Optional

from dvc.objects.errors import ObjectFormatError
from dvc.objects.file import HashFile

if TYPE_CHECKING:
    from dvc.fs.base import FileSystem
    from dvc.hash_info import HashInfo
    from dvc.objects.db import ObjectDB
    from dvc.types import AnyPath

logger = logging.getLogger(__name__)


class ReferenceHashFile(HashFile):
    PARAM_PATH = "path"
    PARAM_HASH = "hash"
    PARAM_CHECKSUM = "checksum"
    PARAM_FS_CONFIG = "fs_config"

    def __init__(
        self,
        fs_path: "AnyPath",
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
        from dvc.fs.repo import RepoFileSystem

        # NOTE: dumping reference FS's this way is insecure, as the
        # fully parsed remote FS config will include credentials
        #
        # ReferenceHashFiles should currently only be serialized in
        # memory and not to disk
        fs_path = self.fs_path
        if isinstance(self.fs, RepoFileSystem):
            fs_path = self.fs.path.relpath(fs_path, self.fs.root_dir)

        dict_ = {
            self.PARAM_PATH: fs_path,
            self.PARAM_HASH: self.hash_info,
            self.PARAM_CHECKSUM: self.checksum,
            self.PARAM_FS_CONFIG: self.config_tuple(self.fs),
        }
        try:
            return pickle.dumps(dict_)
        except pickle.PickleError as exc:
            raise ObjectFormatError(f"Could not pickle {self}") from exc

    @classmethod
    def from_bytes(cls, data: bytes, fs_cache: Optional[dict] = None):
        from dvc.fs import get_fs_cls
        from dvc.fs.repo import RepoFileSystem

        try:
            dict_ = pickle.loads(data)
        except pickle.PickleError as exc:
            raise ObjectFormatError("ReferenceHashFile is corrupted") from exc

        try:
            fs_path = dict_[cls.PARAM_PATH]
            hash_info = dict_[cls.PARAM_HASH]
        except KeyError as exc:
            raise ObjectFormatError("ReferenceHashFile is corrupted") from exc

        scheme, config_pairs = dict_.get(cls.PARAM_FS_CONFIG)
        fs = fs_cache.get((scheme, config_pairs)) if fs_cache else None
        if not fs:
            config = dict(config_pairs)
            if RepoFileSystem.PARAM_REPO_URL in config:
                fs = RepoFileSystem(**config)
                fs_path = fs.path.join(fs.root_dir, fs_path)
            else:
                fs_cls = get_fs_cls(config, scheme=scheme)
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
            fs.scheme,
            tuple(
                (key, value)
                for key, value in sorted(
                    fs.config.items(), key=lambda item: item[0]
                )
            ),
        )
