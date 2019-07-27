import logging
import mock
from dvc.progress import progress, ProgressCallback


def test_quiet(caplog, capsys):
    with caplog.at_level(logging.CRITICAL, logger="dvc"):
        progress.clearln()
        assert capsys.readouterr().out == ""


class TestProgressCallback:
    @mock.patch("dvc.progress.progress")
    def test_should_init_reset_progress(self, progress_mock):
        total_files_num = 1

        ProgressCallback(total_files_num)

        assert [mock.call.reset()] == progress_mock.method_calls
