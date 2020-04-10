import logging
import os
import struct
import threading
import zlib
from binascii import unhexlify

from funcy import chunks, concat, split

from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


DEFAULT_PROTOCOL = 1
SUPPORTED_PROTOCOLS = [1]

# Index format (v1) is the following:
#
# (all integer types are little-endian)
#
# Header
# ------
#   protocol_version (32-bit uint): index file protocol version
#   dir_checksum_count (64-bit uint): number of .dir checksums in data section
#   file_checksum_count (64-bit uint): number of file checksums in data section
#   crc - 32-bit CRC32 checksum of uncompressed data
#
# Data (total length = 16 * dir_checksum_count * file_checksum_count bytes)
# ----------------------
#   array of <dir_checksum_count> 128-bit MD5 .dir checksums
#   array of <file_checksum_count> 128-bit MD5 file checksums
_header_v1 = struct.Struct("<IQQI")


def _verify_protocol(protocol=None):
    if protocol is None:
        protocol = DEFAULT_PROTOCOL
    if protocol not in SUPPORTED_PROTOCOLS:
        raise DvcException(
            "unsupported remote index protocol version: {}".format(protocol)
        )
    return protocol


def dump(dir_checksums, file_checksums, fobj, protocol=None):
    """Write specified checksums to the open file object ``fobj``."""
    protocol = _verify_protocol(protocol)
    _dump_v1(dir_checksums, file_checksums, fobj)


def _dump_v1(dir_checksums, file_checksums, fobj):
    pos = fobj.tell()
    # write header
    fobj.write(_header_v1.pack(1, len(dir_checksums), len(file_checksums), 0))
    # write 1024 MD5 checksums (16kB chunks) at a time
    crc = 0
    data_checksums = concat(
        (csum[:32] for csum in dir_checksums), file_checksums
    )
    for checksums in chunks(1024, data_checksums):
        data = unhexlify("".join(checksums))
        crc = zlib.crc32(data, crc)
        fobj.write(data)
    endpos = fobj.tell()
    # write final CRC value
    fobj.seek(pos + _header_v1.size - 4)
    fobj.write(struct.pack("<I", crc))
    fobj.seek(endpos)


def load(fobj, protocol=None, dir_suffix=".dir"):
    """Read index checksums from the open file object ``fobj``.

    Returns a 2-tuple of (dir_checksums, file_checksums).
    """
    data = fobj.read(4)
    (protocol,) = struct.unpack("<I", data)
    _verify_protocol(protocol)
    fobj.seek(0)
    return _load_v1(fobj, dir_suffix)


def _load_v1(fobj, dir_suffix=""):
    try:
        protocol, dir_count, file_count, file_crc = _header_v1.unpack(
            fobj.read(_header_v1.size)
        )
    except struct.error as exc:
        raise DvcException("Invalid v1 remote index file: {}".format(exc))
    dir_checksums = set()
    file_checksums = set()
    crc = 0

    def read_chunks(num_checksums):
        bytes_remaining = 16 * num_checksums
        # read 1024 checksums (16kB chunks) at a time
        while bytes_remaining > 0:
            data = fobj.read(min(bytes_remaining, 16384))
            bytes_remaining -= len(data)
            yield data

    for data in read_chunks(dir_count):
        crc = zlib.crc32(data, crc)
        checksums = chunks(32, data.hex())
        dir_checksums.update(checksum + dir_suffix for checksum in checksums)
    for data in read_chunks(file_count):
        crc = zlib.crc32(data, crc)
        checksums = chunks(32, data.hex())
        file_checksums.update(checksums)

    if crc != file_crc:
        raise DvcException("Remote index file failed CRC check")

    return dir_checksums, file_checksums


class RemoteIndex(object):
    """Class for locally indexing remote checksums.

    Args:
        repo: repo for this remote index.
        name: name for this index. If name is provided, this index will be
            loaded from and saved to ``.dvc/tmp/index/{name}.idx``.
            If name is not provided (i.e. for local remotes), this index will
            be kept in memory but not saved to disk.
    """

    INDEX_SUFFIX = ".idx"

    def __init__(self, repo, name=None, dir_suffix=".dir"):
        if name:
            self.path = os.path.join(
                repo.index_dir, "{}{}".format(name, self.INDEX_SUFFIX)
            )
        else:
            self.path = None
        self.dir_suffix = dir_suffix
        self.lock = threading.RLock()
        self._dir_checksums = set()
        self._file_checksums = set()
        self.modified = False
        self.load()

    def __iter__(self):
        return iter(self.checksums)

    @property
    def checksums(self):
        return self._dir_checksums | self._file_checksums

    def is_dir_checksum(self, checksum):
        return checksum.endswith(self.dir_suffix)

    def load(self):
        """(Re)load this index from disk."""
        if self.path and os.path.isfile(self.path):
            self.lock.acquire()
            try:
                with open(self.path, "rb") as fobj:
                    self._dir_checksums, self._file_checksums = load(
                        fobj, dir_suffix=self.dir_suffix
                    )
                self.modified = False
            except IOError as exc:
                logger.error(
                    "Failed to load remote index file '{}'. "
                    "Remote will be re-indexed: {}".format(self.path, exc)
                )
            finally:
                self.lock.release()

    def save(self):
        """Save this index to disk."""
        if self.path and self.modified:
            self.lock.acquire()
            try:
                with open(self.path, "wb") as fobj:
                    dump(self._dir_checksums, self._file_checksums, fobj)
                self.modified = False
            except IOError as exc:
                logger.error(
                    "Failed to save remote index file '{}': {}".format(
                        self.path, exc
                    )
                )
            finally:
                self.lock.release()

    def invalidate(self):
        """Invalidate this index (to force re-indexing later)."""
        self.lock.acquire()
        self._dir_checksums.clear()
        self._file_checksums.clear()
        self.modified = True
        if self.path and os.path.isfile(self.path):
            try:
                os.unlink(self.path)
            except IOError as exc:
                logger.error(
                    "Failed to remove remote index file '{}': {}".format(
                        self.path, exc
                    )
                )
        self.lock.release()

    def remove(self, checksum):
        if checksum in self._checksums:
            self.lock.acquire()
            self._checksums.remove(checksum)
            self.modified = True
            self.lock.release()

    def replace(self, checksums):
        """Replace the full contents of this index with ``checksums``.

        Changes to the index will not be written to disk.
        """
        self.lock.acquire()
        self._dir_checksums = set()
        self._file_checksums = set()
        self.update(checksums)
        self.lock.release()

    def update(self, *checksums):
        """Update this index, adding elements from ``checksums``.

        Changes to the index will not be written to disk.
        """
        dir_checksums, file_checksums = split(self.is_dir_checksum, *checksums)
        self.lock.acquire()
        self._dir_checksums.update(dir_checksums)
        self._file_checksums.update(file_checksums)
        self.modified = True
        self.lock.release()
