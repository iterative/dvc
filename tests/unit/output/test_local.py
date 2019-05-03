from mock import patch

from dvc.stage import Stage
from dvc.output import OutputLOCAL

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


class TestGetFilesNumber(TestDvc):
    def _get_output(self):
        stage = Stage(self.dvc)
        return OutputLOCAL(stage, "path")

    def test_return_0_on_no_cache(self):
        o = self._get_output()
        o.use_cache = False
        self.assertEqual(0, o.get_files_number())

    @patch.object(OutputLOCAL, "checksum", "12345678.dir")
    @patch.object(OutputLOCAL, "dir_cache", [{"md5": "asdf"}, {"md5": "qwe"}])
    def test_return_mutiple_for_dir(self):
        o = self._get_output()

        self.assertEqual(2, o.get_files_number())

    @patch.object(OutputLOCAL, "checksum", "12345678")
    @patch.object(OutputLOCAL, "is_dir_checksum", False)
    def test_return_1_on_single_file_cache(self):
        o = self._get_output()

        self.assertEqual(1, o.get_files_number())
