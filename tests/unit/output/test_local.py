import os

from mock import patch
from dvc.output import OutputLOCAL
from dvc.remote.local import RemoteLOCAL
from dvc.stage import Stage
from dvc.utils import relpath
from tests.basic_env import TestDvc


class TestOutputLOCAL(TestDvc):
    def _get_cls(self):
        return OutputLOCAL

    def _get_output(self):
        stage = Stage(self.dvc)
        return self._get_cls()(stage, "path", cache=False)

    def test_save_missing(self):
        o = self._get_output()
        with patch.object(o.remote, "exists", return_value=False):
            with self.assertRaises(o.DoesNotExistError):
                o.save()


def test_str_workdir_outside_repo(erepo):
    stage = Stage(erepo.dvc)
    output = OutputLOCAL(stage, "path", cache=False)

    assert relpath("path", erepo.dvc.root_dir) == str(output)


def test_str_workdir_inside_repo(dvc_repo):
    stage = Stage(dvc_repo)
    output = OutputLOCAL(stage, "path", cache=False)

    assert "path" == str(output)

    stage = Stage(dvc_repo, wdir="some_folder")
    output = OutputLOCAL(stage, "path", cache=False)

    assert os.path.join("some_folder", "path") == str(output)


def test_str_on_absolute_path(dvc_repo):
    stage = Stage(dvc_repo)

    path = os.path.abspath(os.path.join("path", "to", "file"))
    output = OutputLOCAL(stage, path, cache=False)

    assert path == str(output)


class TestGetFilesNumber(TestDvc):
    def _get_output(self):
        stage = Stage(self.dvc)
        return OutputLOCAL(stage, "path")

    def test_return_0_on_no_cache(self):
        o = self._get_output()
        o.use_cache = False
        self.assertEqual(0, o.get_files_number())

    @patch.object(OutputLOCAL, "checksum", "12345678.dir")
    @patch.object(
        RemoteLOCAL,
        "get_dir_cache",
        return_value=[{"md5": "asdf"}, {"md5": "qwe"}],
    )
    def test_return_multiple_for_dir(self, mock_get_dir_cache):
        o = self._get_output()

        self.assertEqual(2, o.get_files_number())

    @patch.object(OutputLOCAL, "checksum", "12345678")
    @patch.object(OutputLOCAL, "is_dir_checksum", False)
    def test_return_1_on_single_file_cache(self):
        o = self._get_output()

        self.assertEqual(1, o.get_files_number())
