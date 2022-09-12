import os

import pytest

from dvc.fs import localfs
from dvc.lock import LockError
from dvc.rwlock import (
    RWLockFileCorruptedError,
    RWLockFileFormatError,
    _edit_rwlock,
    rwlock,
)


def test_rwlock(tmp_path):
    path = os.fspath(tmp_path)
    foo = "foo"

    with rwlock(path, localfs, "cmd1", [foo], [], False):
        with pytest.raises(LockError):
            with rwlock(path, localfs, "cmd2", [], [foo], False):
                pass

    with rwlock(path, localfs, "cmd1", [], [foo], False):
        with pytest.raises(LockError):
            with rwlock(path, localfs, "cmd2", [foo], [], False):
                pass

    with rwlock(path, localfs, "cmd1", [], [foo], False):
        with pytest.raises(LockError):
            with rwlock(path, localfs, "cmd2", [], [foo], False):
                pass


def test_rwlock_reentrant(tmp_path):
    path = os.fspath(tmp_path)
    foo = "foo"

    with rwlock(path, localfs, "cmd1", [], [foo], False):
        with rwlock(path, localfs, "cmd1", [], [foo], False):
            pass
        with _edit_rwlock(path, localfs, False) as lock:
            assert lock == {
                "read": {},
                "write": {"foo": {"cmd": "cmd1", "pid": os.getpid()}},
            }

    with rwlock(path, localfs, "cmd", [foo], [], False):
        with rwlock(path, localfs, "cmd", [foo], [], False):
            pass
        with _edit_rwlock(path, localfs, False) as lock:
            assert lock == {
                "read": {"foo": [{"cmd": "cmd", "pid": os.getpid()}]},
                "write": {},
            }


def test_rwlock_edit_is_guarded(tmp_path, mocker):
    # patching to speedup tests
    mocker.patch("dvc.lock.DEFAULT_TIMEOUT", 0.01)

    path = os.fspath(tmp_path)

    with _edit_rwlock(path, localfs, False):
        with pytest.raises(LockError):
            with _edit_rwlock(path, localfs, False):
                pass


def test_rwlock_subdirs(tmp_path):
    path = os.fspath(tmp_path)
    foo = "foo"
    subfoo = os.path.join("foo", "subfoo")

    with rwlock(path, localfs, "cmd1", [foo], [], False):
        with pytest.raises(LockError, match=r"subfoo(.|\n)*cmd1"):
            with rwlock(path, localfs, "cmd2", [], [subfoo], False):
                pass

    with rwlock(path, localfs, "cmd1", [], [subfoo], False):
        with pytest.raises(LockError, match=r"'foo'(.|\n)*cmd1"):
            with rwlock(path, localfs, "cmd2", [foo], [], False):
                pass

    with rwlock(path, localfs, "cmd1", [], [subfoo], False):
        with pytest.raises(LockError):
            with rwlock(path, localfs, "cmd2", [], [foo], False):
                pass

    with rwlock(path, localfs, "cmd1", [subfoo], [], False):
        with rwlock(path, localfs, "cmd2", [foo], [], False):
            pass


def test_broken_rwlock(tmp_path):
    dir_path = os.fspath(tmp_path)
    path = tmp_path / "rwlock"

    path.write_text('{"broken": "format"}', encoding="utf-8")
    with pytest.raises(RWLockFileFormatError):
        with _edit_rwlock(dir_path, localfs, False):
            pass

    path.write_text("{broken json", encoding="utf-8")
    with pytest.raises(RWLockFileCorruptedError):
        with _edit_rwlock(dir_path, localfs, False):
            pass
