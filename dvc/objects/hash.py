import hashlib
import io
import logging
from typing import TYPE_CHECKING, Any, BinaryIO, Dict, Optional, Tuple

from .fs.callbacks import DEFAULT_CALLBACK, Callback, TqdmCallback
from .fs.implementations.local import localfs
from .fs.utils import is_exec
from .hash_info import HashInfo
from .istextfile import DEFAULT_CHUNK_SIZE, istextblock
from .meta import Meta

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from .fs.base import AnyFSPath, FileSystem
    from .state import StateBase


def dos2unix(data: bytes) -> bytes:
    return data.replace(b"\r\n", b"\n")


def get_hasher(name: str) -> "hashlib._Hash":
    try:
        return getattr(hashlib, name)()
    except AttributeError:
        return hashlib.new(name)


class HashStreamFile(io.IOBase):
    def __init__(
        self,
        fobj: BinaryIO,
        hash_name: str = "md5",
        text: Optional[bool] = None,
    ) -> None:
        self.fobj = fobj
        self.total_read = 0
        self.hasher = get_hasher(hash_name)
        self.is_text: Optional[bool] = text
        super().__init__()

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.fobj.tell()

    def read(self, n=-1) -> bytes:
        chunk = self.fobj.read(n)
        if self.is_text is None and chunk:
            # do we need to buffer till the DEFAULT_CHUNK_SIZE?
            self.is_text = istextblock(chunk[:DEFAULT_CHUNK_SIZE])

        data = dos2unix(chunk) if self.is_text else chunk
        self.hasher.update(data)
        self.total_read += len(data)
        return chunk

    @property
    def hash_value(self) -> str:
        return self.hasher.hexdigest()

    @property
    def hash_name(self) -> str:
        return self.hasher.name


def fobj_md5(
    fobj: BinaryIO, chunk_size: int = 2**20, text: Optional[bool] = None
) -> str:
    # ideally, we want the heuristics to be applied in a similar way,
    # regardless of the size of the first chunk,
    # for which we may need to buffer till DEFAULT_CHUNK_SIZE.
    assert chunk_size >= DEFAULT_CHUNK_SIZE
    stream = HashStreamFile(fobj, text=text)
    while True:
        data = stream.read(chunk_size)
        if not data:
            break
    return stream.hash_value


def file_md5(
    fname: "AnyFSPath",
    fs: "FileSystem" = localfs,
    callback: "Callback" = DEFAULT_CALLBACK,
    text: Optional[bool] = None,
) -> str:
    size = fs.size(fname) or 0
    callback.set_size(size)
    with fs.open(fname, "rb") as fobj:
        return fobj_md5(callback.wrap_attr(fobj), text=text)


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


def _hash_file(
    fs_path: "AnyFSPath",
    fs: "FileSystem",
    name: str,
    callback: "Callback" = DEFAULT_CALLBACK,
) -> Tuple["str", Dict[str, Any]]:
    info = _adapt_info(fs.info(fs_path), fs.protocol)

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
    set_size = Callback.set_size

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
    callback: "Callback" = None,
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
