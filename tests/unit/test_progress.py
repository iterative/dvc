import mock
from unittest import TestCase

from dvc.progress import ProgressCallback


class TestProgressCallback(TestCase):
    @mock.patch("dvc.progress.progress")
    def test_should_init_reset_progress(self, progress_mock):
        total_files_num = 1

        ProgressCallback(total_files_num)

        self.assertEqual([mock.call.reset()], progress_mock.method_calls)
