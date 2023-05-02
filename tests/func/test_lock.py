import pytest

from dvc.cli import main
from dvc.exceptions import DvcException
from dvc.lock import Lock, LockError


def test_with(tmp_dir, dvc, mocker):
    # patching to speedup tests
    mocker.patch("dvc.lock.DEFAULT_TIMEOUT", 0.01)

    lockfile = tmp_dir / dvc.tmp_dir / "lock"
    with Lock(lockfile):
        with pytest.raises(LockError), Lock(lockfile):
            pass


def test_unlock_lock_failed(tmp_dir, dvc, request, mocker):
    # patching to speedup tests
    mocker.patch("dvc.lock.DEFAULT_TIMEOUT", 0.01)

    lockfile = tmp_dir / dvc.tmp_dir / "lock"
    lock = Lock(lockfile)
    lock_ext = Lock(lockfile)

    # It's a common scenario now to have lock unlocked and locked back (e.g. in
    # repro of a stage) in with. We should see LockError exception here.
    with lock:
        lock.unlock()
        lock_ext.lock()  # imitate an external process had time to lock it
        request.addfinalizer(lock_ext.unlock)
        with pytest.raises(LockError):
            lock.lock()


def test_unlock_unlocked_raises():
    lock = Lock("lock")
    with pytest.raises(DvcException, match="Unlock called on an unlocked lock"):
        lock.unlock()


def test_cli(tmp_dir, dvc, mocker, caplog):
    # patching to speedup tests
    mocker.patch("dvc.lock.DEFAULT_TIMEOUT", 0.01)

    expected_error_msg = (
        "Unable to acquire lock. Most likely another DVC process is "
        "running or was terminated abruptly. Check the page "
        "<https://dvc.org/doc/user-guide/troubleshooting#lock-issue> "
        "for other possible reasons and to learn how to resolve this."
    )
    with Lock(tmp_dir / dvc.tmp_dir / "lock"):
        assert main(["add", "foo"]) == 1
    assert expected_error_msg in caplog.text
