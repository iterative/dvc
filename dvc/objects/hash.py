import hashlib
import logging
from typing import TYPE_CHECKING, Any, BinaryIO, Dict, Tuple

from dvc.fs._callback import DEFAULT_CALLBACK, FsspecCallback, TqdmCallback
from dvc.fs.utils import is_exec

from .hash_info import HashInfo
from .istextfile import istextfile
from .meta import Meta

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from dvc.fs.base import AnyFSPath, FileSystem

    from .state import StateBase


def _adapt_info(info: Dict[str, Any], scheme: str) -> Dict[str, Any]:
    if scheme == "s3" and "ETag" in info:
        info["etag"] = info["ETag"].strip('"')
    elif scheme == "gs" and "etag" in info:
        import base64

        info["etag"] = base64.b64decode(info["etag"]).hex()
    elif scheme.startswith("http") and (
        "ETag" in info or "Content-MD5" in info
    ):
        info["checksum"] = info.get("ETag") or info.get("Content-MD5")
    return info


def dos2unix(data: bytes) -> bytes:
    return data.replace(b"\r\n", b"\n")


def _fobj_md5(
    fobj: BinaryIO,
    hash_md5: "hashlib._Hash",
    binary: bool,
    chunk_size: int = 2**20,
) -> None:
    while True:
        data = fobj.read(chunk_size)
        if not data:
            break
        chunk = data if binary else dos2unix(data)
        hash_md5.update(chunk)


def file_md5(
    fname: "AnyFSPath",
    fs: "FileSystem",
    callback: "FsspecCallback" = DEFAULT_CALLBACK,
) -> str:
    """get the (md5 hexdigest, md5 digest) of a file"""

    hash_md5 = hashlib.md5()
    binary = not istextfile(fname, fs=fs)
    size = fs.size(fname) or 0
    callback.set_size(size)
    with fs.open(fname, "rb") as fobj:
        _fobj_md5(callback.wrap_attr(fobj), hash_md5, binary)
    return hash_md5.hexdigest()


def _hash_file(
    fs_path: "AnyFSPath",
    fs: "FileSystem",
    name: str,
    callback: "FsspecCallback" = DEFAULT_CALLBACK,
) -> Tuple["str", Dict[str, Any]]:
    info = _adapt_info(fs.info(fs_path), fs.scheme)

    if name in info:
        assert not info[name].endswith(".dir")
        return info[name], info

    if hasattr(fs, name):
        func = getattr(fs, name)
        return func(fs_path), info

    if name == "md5":
        return file_md5(fs_path, fs, callback=callback), info
    raise NotImplementedError


class LargeFileHashingCallback(TqdmCallback):
    """Callback that only shows progress bar if self.size > LARGE_FILE_SIZE."""

    LARGE_FILE_SIZE = 2**30

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("bytes", True)
        super().__init__(*args, **kwargs)
        self._logged = False

    # TqdmCallback force renders progress bar on `set_size`.
    set_size = FsspecCallback.set_size

    def call(self, hook_name=None, **kwargs):
        if self.size and self.size > self.LARGE_FILE_SIZE:
            if not self._logged:
                desc = self.progress_bar.desc
                logger.info(
                    f"Computing md5 for a large file '{desc}'. "
                    "This is only done once."
                )
                self._logged = True
            super().call()


def hash_file(
    fs_path: "AnyFSPath",
    fs: "FileSystem",
    name: str,
    state: "StateBase" = None,
    callback: "FsspecCallback" = None,
) -> Tuple["Meta", "HashInfo"]:
    if state:
        meta, hash_info = state.get(fs_path, fs)
        if hash_info:
            return meta, hash_info

    cb = callback or LargeFileHashingCallback(desc=fs_path)
    with cb:
        hash_value, info = _hash_file(fs_path, fs, name, callback=cb)
    hash_info = HashInfo(name, hash_value)
    if state:
        assert ".dir" not in hash_info.value
        state.save(fs_path, fs, hash_info)

    meta = Meta(size=info["size"], isexec=is_exec(info.get("mode", 0)))
    return meta, hash_info
