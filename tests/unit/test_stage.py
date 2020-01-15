import signal
import subprocess
import threading
from unittest import TestCase

import mock
import pytest

from dvc.dependency.repo import DependencyREPO
from dvc.path_info import PathInfo
from dvc.stage import Stage
from dvc.stage import StageUpdateError


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
        import os

        stage = Stage(None, "path")

        stage.wdir = os.path.join("..", "..")
        self.assertEqual(stage.dumpd()["wdir"], "../..")


@pytest.mark.parametrize("add", [True, False])
def test_stage_fname(add):
    out = mock.Mock()
    out.is_in_repo = False
    out.path_info = PathInfo("path/to/out.txt")
    fname = Stage._stage_fname([out], add)
    assert fname == "out.txt.dvc"


def test_stage_update(mocker):
    dep = DependencyREPO({"url": "example.com"}, None, "dep_path")
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

    dvc.run(cmd="path")

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
