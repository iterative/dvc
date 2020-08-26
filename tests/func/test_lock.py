import pytest

from dvc.lock import FAILED_TO_LOCK_MESSAGE, Lock, LockError
from dvc.main import main


def test_with(tmp_dir, dvc, mocker):
    # patching to speedup tests
    mocker.patch("dvc.lock.DEFAULT_TIMEOUT", 0.01)

    lockfile = tmp_dir / dvc.tmp_dir / "lock"
    with Lock(lockfile):
        with pytest.raises(LockError), Lock(lockfile):
            pass


def test_cli(tmp_dir, dvc, mocker, caplog):
    # patching to speedup tests
    mocker.patch("dvc.lock.DEFAULT_TIMEOUT", 0.01)

    with Lock(tmp_dir / dvc.tmp_dir / "lock"):
        assert main(["add", "foo"]) == 1
    assert FAILED_TO_LOCK_MESSAGE in caplog.text
