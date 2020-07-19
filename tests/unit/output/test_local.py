import os

from mock import patch

from dvc.cache.local import LocalCache
from dvc.output import LocalOutput
from dvc.stage import Stage
from dvc.utils import relpath
from tests.basic_env import TestDvc


class TestLocalOutput(TestDvc):
    def _get_cls(self):
        return LocalOutput

    def _get_output(self):
        stage = Stage(self.dvc)
        return self._get_cls()(stage, "path", cache=False)

    def test_save_missing(self):
        o = self._get_output()
        with patch.object(o.tree, "exists", return_value=False):
            with self.assertRaises(o.DoesNotExistError):
                o.save()


def test_str_workdir_outside_repo(erepo_dir):
    stage = Stage(erepo_dir.dvc)
    output = LocalOutput(stage, "path", cache=False)

    assert relpath("path", erepo_dir.dvc.root_dir) == str(output)


def test_str_workdir_inside_repo(dvc):
    stage = Stage(dvc)
    output = LocalOutput(stage, "path", cache=False)

    assert "path" == str(output)

    stage = Stage(dvc, wdir="some_folder")
    output = LocalOutput(stage, "path", cache=False)

    assert os.path.join("some_folder", "path") == str(output)


def test_str_on_local_absolute_path(dvc):
    stage = Stage(dvc)

    rel_path = os.path.join("path", "to", "file")
    abs_path = os.path.abspath(rel_path)
    output = LocalOutput(stage, abs_path, cache=False)

    assert output.def_path == rel_path
    assert output.path_info.fspath == abs_path
    assert str(output) == rel_path


def test_str_on_external_absolute_path(dvc):
    stage = Stage(dvc)

    rel_path = os.path.join("..", "path", "to", "file")
    abs_path = os.path.abspath(rel_path)
    output = LocalOutput(stage, abs_path, cache=False)

    assert output.def_path == abs_path
    assert output.path_info.fspath == abs_path
    assert str(output) == abs_path


class TestGetFilesNumber(TestDvc):
    def _get_output(self):
        stage = Stage(self.dvc)
        return LocalOutput(stage, "path")

    def test_return_0_on_no_cache(self):
        o = self._get_output()
        o.use_cache = False
        self.assertEqual(0, o.get_files_number())

    @patch.object(LocalOutput, "checksum", "12345678.dir")
    @patch.object(
        LocalCache,
        "get_dir_cache",
        return_value=[{"md5": "asdf"}, {"md5": "qwe"}],
    )
    def test_return_multiple_for_dir(self, _mock_get_dir_cache):
        o = self._get_output()

        self.assertEqual(2, o.get_files_number())

    @patch.object(LocalOutput, "checksum", "12345678")
    @patch.object(LocalOutput, "is_dir_checksum", False)
    def test_return_1_on_single_file_cache(self):
        o = self._get_output()

        self.assertEqual(1, o.get_files_number())
