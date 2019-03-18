from dvc.exceptions import CacheNotFoundException
from dvc.remote import RemoteLOCAL
from tests.basic_env import TestDvc
from mock import patch, Mock


class TestGetFilesNumber(TestDvc):
    test_checksum = "12345"

    def setUp(self):
        super(TestGetFilesNumber, self).setUp()
        self.remote = self.dvc.cache.local

    @patch.object(RemoteLOCAL, "get", return_value=None)
    def test_shoud_raise_on_no_cache(self, _):
        with self.assertRaises(CacheNotFoundException):
            self.remote.get_files_number(self.test_checksum)

    @patch.object(RemoteLOCAL, "get", return_value=Mock())
    @patch.object(RemoteLOCAL, "is_dir_cache", return_value=False)
    def test_should_return_1_on_not_dir_cache(self, _, __):
        result = self.remote.get_files_number(self.test_checksum)
        self.assertEqual(1, result)

    @patch.object(RemoteLOCAL, "get", return_value=Mock())
    @patch.object(RemoteLOCAL, "is_dir_cache", return_value=True)
    @patch.object(
        RemoteLOCAL,
        "load_dir_cache",
        return_value=[
            {"md5": "md5-1", "relpath": "relpath1"},
            {"md5": "md5-2", "relpath": "relpath2"},
        ],
    )
    def test_should_return_dir_content_number_on_dir_cache(self, _, __, ___):
        result = self.remote.get_files_number(self.test_checksum)
        self.assertEqual(2, result)
