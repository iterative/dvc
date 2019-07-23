from dvc.path_info import PathInfo
from dvc.stage import Stage, StageUpdateError
from dvc.dependency.repo import DependencyREPO

import mock
import pytest
from unittest import TestCase


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
    dep = DependencyREPO({"url": "example.com"}, None, None)
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
