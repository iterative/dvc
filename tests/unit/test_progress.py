import mock
from unittest import TestCase

from dvc.progress import progress, progress_aware, ProgressCallback


class TestProgressAware(TestCase):
    @mock.patch("sys.stdout.isatty", return_value=True)
    @mock.patch("dvc.progress.Progress.print")
    def test(self, mock_print, _):
        # progress is a global object, can be shared between tests when
        # run in multi-threading environment with pytest
        progress.reset()
        function = progress_aware(lambda: None)

        function()
        # first - called once for progress.clearln()
        mock_print.assert_called_once()

        progress.update_target("testing", 0, 100)
        function()
        # second - progress.clearln() on refresh on update_target
        # third - progress.print on refresh on update_target
        # fourth - progress.clearln()
        self.assertEqual(4, mock_print.call_count)

        progress.finish_target("testing")


class TestProgressCallback(TestCase):
    @mock.patch("dvc.progress.progress")
    def test_should_init_reset_progress(self, progress_mock):
        total_files_num = 1

        ProgressCallback(total_files_num)

        self.assertEqual([mock.call.reset()], progress_mock.method_calls)
