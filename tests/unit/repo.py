import os

from tests.basic_env import TestDvc


class TestIsDvcInternal(TestDvc):
    def test_return_false_on_non_dvc_file(self):
        path = os.path.join("path", "to-non-.dvc", "file")
        self.assertFalse(self.dvc.is_dvc_internal(path))

    def test_return_true_on_dvc_internal_file(self):
        path = os.path.join("path", "to", ".dvc", "file")
        self.assertTrue(path)
