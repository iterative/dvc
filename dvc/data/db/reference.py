import logging
import os
from typing import TYPE_CHECKING, Dict

from dvc_objects.db import ObjectDB
from dvc_objects.errors import ObjectFormatError
from dvc_objects.fs import Schemes

from ..reference import ReferenceHashFile

if TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath, FileSystem
    from dvc_objects.fs.callbacks import Callback
    from dvc_objects.hash_info import HashInfo

logger = logging.getLogger(__name__)


class ReferenceObjectDB(ObjectDB):
    """Reference ODB.

    File objects are stored as ReferenceHashFiles which reference paths outside
    of the staging ODB fs. Tree objects are stored natively.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._fs_cache: Dict[tuple, "FileSystem"] = {}
        self._obj_cache: Dict["HashInfo", "ReferenceHashFile"] = {}

    def get(self, hash_info: "HashInfo"):
        if hash_info.isdir:
            return super().get(hash_info)
        try:
            return self._obj_cache[hash_info]
        except KeyError:
            pass
        fs_path = self.hash_to_path(hash_info.value)
        try:
            ref_file = ReferenceHashFile.from_bytes(
                self.fs.cat_file(fs_path),
                fs_cache=self._fs_cache,
            )
        except OSError:
            raise FileNotFoundError
        try:
            ref_file.check(self, check_hash=False)
        except ObjectFormatError:
            self.fs.remove(fs_path)
            raise
        self._obj_cache[hash_info] = ref_file
        return ref_file

    def _add_file(
        self,
        from_fs: "FileSystem",
        from_info: "AnyFSPath",
        to_info: "AnyFSPath",
        hash_info: "HashInfo",
        hardlink: bool = False,
        callback: "Callback" = None,
    ):
        if hash_info.isdir:
            return super()._add_file(
                from_fs,
                from_info,
                to_info,
                hash_info,
                hardlink=hardlink,
                callback=callback,
            )

        self.makedirs(self.fs.path.parent(to_info))
        ref_file = ReferenceHashFile(from_info, from_fs, hash_info)
        self._obj_cache[hash_info] = ref_file
        try:
            self.fs.pipe_file(to_info, ref_file.to_bytes())
        except OSError as exc:
            if isinstance(exc, FileExistsError) or (
                os.name == "nt"
                and exc.__context__
                and isinstance(exc.__context__, FileExistsError)
            ):
                logger.debug("'%s' file already exists, skipping", to_info)
            else:
                raise
        if from_fs.protocol != Schemes.LOCAL:
            self._fs_cache[ReferenceHashFile.config_tuple(from_fs)] = from_fs
