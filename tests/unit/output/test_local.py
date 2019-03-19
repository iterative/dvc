from mock import patch, Mock, call

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


class TestChecksumChanged(TestDvc):
    def _get_output(self):
        stage = Stage(self.dvc)
        return OutputLOCAL(stage, "path")

    def test_checksum_changed(self):
        o = self._get_output()

        with patch.object(OutputLOCAL, "checksum", "a"):
            o.cached_checksum = "b"
            self.assertTrue(o.checksum_changed())

    @patch.object(OutputLOCAL, "checksum", "a")
    def test_checksum_unchanged(self):
        o = self._get_output()

        o.cached_checksum = "a"
        self.assertFalse(o.checksum_changed())


class TestGetFilesNumber(TestDvc):
    def _get_output(self):
        stage = Stage(self.dvc)
        return OutputLOCAL(stage, "path")

    @patch.object(OutputLOCAL, "cache", None)
    def test_return_0_on_no_cache(self):
        o = self._get_output()

        self.assertEqual(0, o.get_files_number())

    @patch.object(OutputLOCAL, "cache", "cache")
    @patch.object(OutputLOCAL, "is_dir_cache", True)
    @patch.object(
        OutputLOCAL,
        "get_dir_cache",
        return_value=[{"md5": "asdf"}, {"md5": "qwe"}],
    )
    def test_return_mutiple_for_dir(self, _):
        o = self._get_output()

        self.assertEqual(2, o.get_files_number())

    @patch.object(OutputLOCAL, "cache", "cache")
    @patch.object(OutputLOCAL, "is_dir_cache", False)
    def test_return_1_on_single_file_cache(self):
        o = self._get_output()

        self.assertEqual(1, o.get_files_number())


class TestGetDirCache(TestDvc):
    checksum = "12345"

    def _get_output(self):
        stage = Stage(self.dvc)
        return OutputLOCAL(stage, "path")

    @patch.object(OutputLOCAL, "checksum", checksum)
    def test_reload_on_no_dir_cache(self):
        o = self._get_output()
        o.dir_cache = None

        dir_cache_mock_new = Mock()
        with patch.object(
            o.repo.cache.local,
            "load_dir_cache",
            return_value=dir_cache_mock_new,
        ) as load_patch:
            self.assertEqual(dir_cache_mock_new, o.get_dir_cache())
            self.assertEqual([call(self.checksum)], load_patch.mock_calls)

        self.assertEqual(self.checksum, o.cached_checksum)

    @patch.object(OutputLOCAL, "checksum", checksum)
    @patch.object(OutputLOCAL, "checksum_changed", return_value=True)
    def test_reload_on_checksum_changed(self, _):
        o = self._get_output()
        dir_cache_mock_old = Mock()
        dir_cache_mock_new = Mock()
        o.dir_cache = dir_cache_mock_old

        with patch.object(
            o.repo.cache.local,
            "load_dir_cache",
            return_value=dir_cache_mock_new,
        ) as load_patch:
            self.assertEqual(dir_cache_mock_new, o.get_dir_cache())
            self.assertEqual([call(self.checksum)], load_patch.mock_calls)

        self.assertEqual(self.checksum, o.cached_checksum)

    @patch.object(OutputLOCAL, "checksum_changed", return_value=False)
    def test_should_not_reload(self, _):
        o = self._get_output()
        dir_cache_mock_old = Mock()
        o.dir_cache = dir_cache_mock_old

        with patch.object(
            o.repo.cache.local,
            "load_dir_cache",
            return_value=dir_cache_mock_old,
        ) as load_patch:
            self.assertEqual([], load_patch.mock_calls)

        self.assertEqual(dir_cache_mock_old, o.get_dir_cache())
