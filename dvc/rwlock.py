import json
import os
from collections import defaultdict
from contextlib import contextmanager

import psutil
from voluptuous import Invalid, Optional, Required, Schema

from dvc.log import logger

from .exceptions import DvcException
from .fs import localfs
from .lock import make_lock
from .utils import relpath

logger = logger.getChild(__name__)


INFO_SCHEMA = {Required("pid"): int, Required("cmd"): str}

SCHEMA = Schema(
    {
        Optional("write", default={}): {str: INFO_SCHEMA},
        Optional("read", default={}): {str: [INFO_SCHEMA]},
    }
)

RWLOCK_FILE = "rwlock"
RWLOCK_LOCK = "rwlock.lock"


class RWLockFileCorruptedError(DvcException):
    def __init__(self, path):
        super().__init__(
            f"Unable to read RWLock-file {relpath(path)!r}. JSON structure is corrupted"
        )


class RWLockFileFormatError(DvcException):
    def __init__(self, path):
        super().__init__(f"RWLock-file {relpath(path)!r} format error.")


@contextmanager
def _edit_rwlock(lock_dir, fs, hardlink):
    path = fs.join(lock_dir, RWLOCK_FILE)

    rwlock_guard = make_lock(
        fs.join(lock_dir, RWLOCK_LOCK),
        tmp_dir=lock_dir,
        hardlink_lock=hardlink,
    )
    with rwlock_guard:
        try:
            with fs.open(path, encoding="utf-8") as fobj:
                lock = SCHEMA(json.load(fobj))
        except FileNotFoundError:
            lock = SCHEMA({})
        except json.JSONDecodeError as exc:
            raise RWLockFileCorruptedError(path) from exc
        except Invalid as exc:
            raise RWLockFileFormatError(path) from exc
        lock["read"] = defaultdict(list, lock["read"])
        lock["write"] = defaultdict(dict, lock["write"])
        yield lock
        with fs.open(path, "w", encoding="utf-8") as fobj:
            json.dump(lock, fobj)


def _infos_to_str(infos):
    return "\n".join(
        "  (PID {}): {}".format(info["pid"], info["cmd"]) for info in infos
    )


def _check_blockers(tmp_dir, lock, info, *, mode, waiters):  # noqa: C901, PLR0912
    from .lock import LockError

    non_existing_pid = set()

    blockers = []
    to_release = defaultdict(list)
    for path, infos in lock[mode].items():
        for waiter_path in waiters:
            if localfs.overlaps(waiter_path, path):
                break
        else:
            continue

        infos = infos if isinstance(infos, list) else [infos]
        for blocker in infos:
            if blocker == info:
                continue

            pid = int(blocker["pid"])

            if pid in non_existing_pid:
                pass
            elif psutil.pid_exists(pid):
                blockers.append(blocker)
                continue
            else:
                non_existing_pid.add(pid)
                cmd = blocker["cmd"]
                logger.warning(
                    (
                        "Process '%s' with (Pid %s), in RWLock-file '%s'"
                        " had been killed. Auto removed it from the lock file."
                    ),
                    cmd,
                    pid,
                    relpath(path),
                )
            to_release[json.dumps(blocker, sort_keys=True)].append(path)

    if to_release:
        for info_json, path_list in to_release.items():
            info = json.loads(info_json)
            if mode == "read":
                _release_read(lock, info, path_list)
            elif mode == "write":
                _release_write(lock, info, path_list)

    if blockers:
        raise LockError(
            f"'{waiter_path}' is busy, it is being blocked by:\n"
            f"{_infos_to_str(blockers)}\n"
            "\n"
            "If there are no processes with such PIDs, you can manually "
            f"remove '{tmp_dir}/rwlock' and try again."
        )


def _acquire_read(lock, info, paths):
    changes = []

    lock["read"] = lock.get("read", defaultdict(list))

    for path in paths:
        readers = lock["read"][path]
        if info in readers:
            continue

        changes.append(path)
        readers.append(info)

    return changes


def _acquire_write(lock, info, paths):
    changes = []

    lock["write"] = lock.get("write", defaultdict(dict))

    for path in paths:
        if lock["write"][path] == info:
            continue

        changes.append(path)
        lock["write"][path] = info

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
def rwlock(tmp_dir, fs, cmd, read, write, hardlink):
    """Create non-thread-safe RWLock for file paths.

    Args:
        tmp_dir (str): existing directory where to create the rwlock file.
        fs (FileSystem): fs instance that tmp_dir belongs to.
        cmd (str): command that will be working on these file path.
        read ([str]): file paths that are going to be read.
        write ([str]): file paths that are going to be written.
        hardlink (bool): use hardlink lock to guard rwlock file when on edit.

    Raises:
        LockError: raised if file paths we want to read is being written to by
            another command or if file paths we want to write is being written
            to or read from by another command.
        RWLockFileCorruptedError: raised if rwlock file is not a valid JSON.
        RWLockFileFormatError: raised if rwlock file is a valid JSON, but
            has internal format that doesn't pass our schema validation.
    """
    info = {"pid": os.getpid(), "cmd": cmd}

    with _edit_rwlock(tmp_dir, fs, hardlink) as lock:
        _check_blockers(tmp_dir, lock, info, mode="write", waiters=read + write)
        _check_blockers(tmp_dir, lock, info, mode="read", waiters=write)

        rchanges = _acquire_read(lock, info, read)
        wchanges = _acquire_write(lock, info, write)

    try:
        yield
    finally:
        with _edit_rwlock(tmp_dir, fs, hardlink) as lock:
            _release_write(lock, info, wchanges)
            _release_read(lock, info, rchanges)
