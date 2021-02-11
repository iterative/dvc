from dvc.hash_info import HashInfo
from dvc.utils import file_md5

from .base import BaseTree


class MemoryTree(BaseTree):
    scheme = "local"
    PARAM_CHECKSUM = "md5"

    def __init__(self, repo, config):
        from fsspec.implementations.memory import MemoryFileSystem

        super().__init__(repo, config)

        self.fs = MemoryFileSystem()

    def exists(self, path_info, use_dvcignore=True):
        return self.fs.exists(path_info.path)

    def open(self, path_info, mode="r", encoding=None, **kwargs):
        return self.fs.open(
            path_info.fspath, mode=mode, encoding=encoding, **kwargs
        )

    def stat(self, path_info):
        import os

        info = self.fs.info(path_info.fspath)

        return os.stat_result((0, 0, 0, 0, 0, 0, info["size"], 0, 0, 0))

    def get_file_hash(self, path_info, name):
        assert name == self.PARAM_CHECKSUM
        return HashInfo(self.PARAM_CHECKSUM, file_md5(path_info, self))

    def walk_files(self, path_info, **kwargs):
        raise NotImplementedError
