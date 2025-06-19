import multiprocessing
import time

import pytest

from dvc.cli import main
from dvc.exceptions import DvcException
from dvc.lock import Lock, LockError, make_lock


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


def hold_lock(lockfile_path, hold_seconds):
    lock = make_lock(lockfile_path)
    with lock:
        time.sleep(hold_seconds)


def try_lock_with_wait(lockfile_path, wait, result_queue):
    try:
        lock = make_lock(lockfile_path, wait=wait)
        with lock:
            result_queue.put("acquired")
    except LockError as e:
        result_queue.put(f"error: {e}")


def test_lock_waits_when_requested(tmp_path):
    lockfile = tmp_path / "lock"

    # Process 1 holds the lock for 2 seconds
    p1 = multiprocessing.Process(target=hold_lock, args=(lockfile, 2))
    p1.start()
    time.sleep(0.2)  # let p1 acquire lock

    # Process 2 will wait for the lock (should succeed)
    q = multiprocessing.Queue()
    p2 = multiprocessing.Process(target=try_lock_with_wait, args=(lockfile, True, q))
    p2.start()
    p2.join(timeout=4)

    result = q.get(timeout=1)
    assert result == "acquired"

    p1.join()
