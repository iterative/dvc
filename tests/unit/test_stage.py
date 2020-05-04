import os
import signal
import subprocess
import threading
from unittest import TestCase

import mock
import pytest

from dvc.dependency.repo import RepoDependency
from dvc.stage import Stage
from dvc.stage.exceptions import StageUpdateError

TEST_STAGE_DICT = {
    "md5": "123456",
    "cmd": "mycmd",
    "outs": [{"path": "a", "md5": "123456789"}],
    "deps": [{"path": "b", "md5": "987654321"}],
}


def test_stage_checksum():
    stage = Stage(None, "path")

    with mock.patch.object(stage, "dumpd", return_value=TEST_STAGE_DICT):
        assert stage._compute_md5() == "e9521a22111493406ea64a88cda63e0b"


def test_wdir_default_ignored():
    stage = Stage(None, "path")
    d = dict(TEST_STAGE_DICT, wdir=".")

    with mock.patch.object(stage, "dumpd", return_value=d):
        assert stage._compute_md5() == "e9521a22111493406ea64a88cda63e0b"


def test_wdir_non_default_is_not_ignored():
    stage = Stage(None, "path")
    d = dict(TEST_STAGE_DICT, wdir="..")

    with mock.patch.object(stage, "dumpd", return_value=d):
        assert stage._compute_md5() == "2ceba15e87f6848aa756502c1e6d24e9"


def test_meta_ignored():
    stage = Stage(None, "path")
    d = dict(TEST_STAGE_DICT, meta={"author": "Suor"})

    with mock.patch.object(stage, "dumpd", return_value=d):
        assert stage._compute_md5() == "e9521a22111493406ea64a88cda63e0b"


class TestPathConversion(TestCase):
    def test(self):
        stage = Stage(None, "path")

        stage.wdir = os.path.join("..", "..")
        self.assertEqual(stage.dumpd()["wdir"], "../..")


def test_stage_update(mocker):
    dep = RepoDependency({"url": "example.com"}, None, "dep_path")
    mocker.patch.object(dep, "update", return_value=None)

    stage = Stage(None, "path", deps=[dep])
    reproduce = mocker.patch.object(stage, "reproduce")
    is_repo_import = mocker.patch(
        __name__ + ".Stage.is_repo_import", new_callable=mocker.PropertyMock
    )

    is_repo_import.return_value = True
    stage.update()
    assert reproduce.called_once_with()

    is_repo_import.return_value = False
    with pytest.raises(StageUpdateError):
        stage.update()


@pytest.mark.skipif(
    not isinstance(threading.current_thread(), threading._MainThread),
    reason="Not running in the main thread.",
)
def test_stage_run_ignore_sigint(dvc, mocker):
    proc = mocker.Mock()
    communicate = mocker.Mock()
    proc.configure_mock(returncode=0, communicate=communicate)
    popen = mocker.patch.object(subprocess, "Popen", return_value=proc)
    signal_mock = mocker.patch("signal.signal")

    dvc.run(cmd="path", single_stage=True)

    assert popen.called_once()
    assert communicate.called_once_with()
    signal_mock.assert_any_call(signal.SIGINT, signal.SIG_IGN)
    assert signal.getsignal(signal.SIGINT) == signal.default_int_handler


def test_always_changed(dvc):
    stage = Stage(dvc, "path", always_changed=True)
    stage.save()
    with dvc.lock:
        assert stage.changed()
        assert stage.status()["path"] == ["always changed"]


def test_stage_cache(tmp_dir, dvc, run_copy, mocker):
    tmp_dir.gen("dep", "dep")
    stage = run_copy("dep", "out", single_stage=True)

    with dvc.lock, dvc.state:
        stage.remove(remove_outs=True, force=True)

    assert not (tmp_dir / "out").exists()
    assert not (tmp_dir / "out.dvc").exists()

    cache_dir = os.path.join(
        dvc.stage_cache.cache_dir,
        "75",
        "75f8a9097d76293ff4b3684d52e4ad0e83686d31196f27eb0b2ea9fd5085565e",
    )
    cache_file = os.path.join(
        cache_dir,
        "c1747e52065bc7801262fdaed4d63f5775e5da304008bd35e2fea4e6b1ccb272",
    )

    assert os.path.isdir(cache_dir)
    assert os.listdir(cache_dir) == [os.path.basename(cache_file)]
    assert os.path.isfile(cache_file)

    run_spy = mocker.spy(stage, "_run")
    checkout_spy = mocker.spy(stage, "checkout")
    with dvc.lock, dvc.state:
        stage.run()

    assert not run_spy.called
    assert checkout_spy.call_count == 1

    assert (tmp_dir / "out").exists()
    assert (tmp_dir / "out").read_text() == "dep"
