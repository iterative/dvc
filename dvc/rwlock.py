from __future__ import unicode_literals

import os
import json

from collections import defaultdict
from contextlib import contextmanager

from voluptuous import Schema, Invalid
from funcy.py3 import lmap, lfilter, lmapcat

from .exceptions import DvcException
from .lock import LockError
from .utils.compat import (
    convert_to_unicode,
    FileNotFoundError,
    JSONDecodeError,
    str,
)
from .utils.fs import relpath

INFO_SCHEMA = {"pid": int, "cmd": str}

SCHEMA = Schema({"write": {str: INFO_SCHEMA}, "read": {str: [INFO_SCHEMA]}})


class RWLockFileCorruptedError(DvcException):
    def __init__(self, path, cause):
        super(RWLockFileCorruptedError, self).__init__(
            "Unable to read RWLock-file '{}'. JSON structure is "
            "corrupted".format(relpath(path)),
            cause=cause,
        )


class RWLockFileFormatError(DvcException):
    def __init__(self, path, cause):
        super(RWLockFileFormatError, self).__init__(
            "RWLock-file '{}' format error.".format(relpath(path)), cause=cause
        )


@contextmanager
def _edit_rwlock(lock_dir):
    path = os.path.join(lock_dir, "rwlock")
    try:
        with open(path, "r") as fobj:
            lock = json.load(fobj)
        lock = SCHEMA(convert_to_unicode(lock))
    except FileNotFoundError:
        lock = SCHEMA({})
    except JSONDecodeError as exc:
        raise RWLockFileCorruptedError(path, cause=exc)
    except Invalid as exc:
        raise RWLockFileFormatError(path, cause=exc)
    lock = defaultdict(dict, lock)
    lock["read"] = defaultdict(list, lock["read"])
    lock["write"] = defaultdict(dict, lock["write"])
    yield lock
    with open(path, "w+") as fobj:
        json.dump(lock, fobj)


def _infos_to_str(infos):
    return "\n".join(
        "  (PID {}): {}".format(info["pid"], info["cmd"]) for info in infos
    )


def _check_no_writers(lock, info, path_infos):
    for path_info in path_infos:
        blocking_urls = lfilter(path_info.overlaps, lock["write"])
        if not blocking_urls:
            continue

        writers = lmap(lock["write"].get, blocking_urls)
        writers = lfilter(lambda i: info != i, writers)
        if not writers:
            continue

        raise LockError(
            "'{}' is busy, it is being written to by:\n{}".format(
                str(path_info), _infos_to_str(writers)
            )
        )


def _check_no_readers(lock, info, path_infos):
    for path_info in path_infos:
        blocking_urls = lfilter(path_info.overlaps, lock["read"])
        if not blocking_urls:
            continue

        readers = lmapcat(lock["read"].get, blocking_urls)
        readers = lfilter(lambda i: info != i, readers)
        if not readers:
            continue

        raise LockError(
            "'{}' is busy, it is being read by:\n{}".format(
                str(path_info), _infos_to_str(readers)
            )
        )


def _acquire_read(lock, info, path_infos):
    changes = []

    for path_info in path_infos:
        url = path_info.url

        readers = lock["read"][url]
        if info in readers:
            continue

        changes.append(url)
        readers.append(info)

    return changes


def _acquire_write(lock, info, path_infos):
    changes = []

    for path_info in path_infos:
        url = path_info.url

        if lock["write"][url] == info:
            continue

        changes.append(url)
        lock["write"][url] = info

    return changes


def _release_write(lock, info, changes):
    for url in changes:
        assert "write" in lock
        assert url in lock["write"]
        assert lock["write"][url] == info
        del lock["write"][url]
        if not lock["write"]:
            del lock["write"]


def _release_read(lock, info, changes):
    for url in changes:
        assert "read" in lock
        assert url in lock["read"]
        assert info in lock["read"][url]
        lock["read"][url].remove(info)
        if not lock["read"][url]:
            del lock["read"][url]
        if not lock["read"]:
            del lock["read"]


@contextmanager
def rwlock(tmp_dir, cmd, read, write):
    """Create non-thread-safe RWLock for PathInfos.

    Args:
        tmp_dir (str): existing directory where to create the rwlock file.
        cmd (str): command that will be working on these PathInfos.
        read ([PathInfo]): PathInfos that are going to be read.
        write ([PathInfo]): PathInfos that are going to be written.

    Raises:
        LockError: raised if PathInfo we want to read is being written to by
            another command or if PathInfo we want to write is being written
            to or read from by another command.
        RWLockFileCorruptedError: raised if rwlock file is not a valid JSON.
        RWLockFileFormatError: raised if rwlock file is a valid JSON, but
            has internal format that doesn't pass our schema validation.
    """
    info = {"pid": os.getpid(), "cmd": cmd}

    with _edit_rwlock(tmp_dir) as lock:
        _check_no_writers(lock, info, read + write)
        _check_no_readers(lock, info, write)

        rchanges = _acquire_read(lock, info, read)
        wchanges = _acquire_write(lock, info, write)

    try:
        yield
    finally:
        with _edit_rwlock(tmp_dir) as lock:
            _release_write(lock, info, wchanges)
            _release_read(lock, info, rchanges)
