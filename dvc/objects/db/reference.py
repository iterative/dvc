import io
import logging
import os
from typing import TYPE_CHECKING

from ..errors import ObjectFormatError
from ..file import ReferenceHashFile
from .base import ObjectDB

if TYPE_CHECKING:
    from dvc.fs.base import BaseFileSystem
    from dvc.hash_info import HashInfo
    from dvc.types import AnyPath

logger = logging.getLogger(__name__)


class ReferenceObjectDB(ObjectDB):
    """Reference ODB.

    File objects are stored as ReferenceHashFiles which reference paths outside
    of the staging ODB fs. Tree objects are stored natively.
    """

    def get(self, hash_info: "HashInfo"):
        if hash_info.isdir:
            return super().get(hash_info)
        path_info = self.hash_to_path(hash_info.value)
        try:
            with self.fs.open(path_info, "rb") as fobj:
                ref_file = ReferenceHashFile.from_bytes(fobj.read())
        except OSError:
            raise FileNotFoundError
        try:
            ref_file.check(self, check_hash=False)
        except ObjectFormatError:
            self.fs.remove(path_info)
            raise
        return ref_file

    def _add_file(
        self,
        from_fs: "BaseFileSystem",
        from_info: "AnyPath",
        to_info: "AnyPath",
        hash_info: "HashInfo",
        move: bool = False,
    ):
        self.makedirs(to_info.parent)
        if hash_info.isdir:
            return super()._add_file(
                from_fs, from_info, to_info, hash_info, move
            )
        ref_file = ReferenceHashFile(from_info, from_fs, hash_info)
        ref_fobj = io.BytesIO(ref_file.to_bytes())
        ref_fobj.seek(0)
        try:
            self.fs.upload_fobj(ref_fobj, to_info)
        except OSError as exc:
            if isinstance(exc, FileExistsError) or (
                os.name == "nt"
                and exc.__context__
                and isinstance(exc.__context__, FileExistsError)
            ):
                logger.debug("'%s' file already exists, skipping", to_info)
            else:
                raise
