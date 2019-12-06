import mock

from dvc.main import main
from dvc.utils.compat import fspath


@mock.patch("dvc.analytics.send")
def test_daemon_analytics(mock_send, tmp_path):
    report = fspath(tmp_path)
    assert 0 == main(["daemon", "analytics", report])

    mock_send.assert_called_with(report)


@mock.patch("dvc.daemon._spawn")
@mock.patch("dvc.analytics.is_enabled", return_value=True)
@mock.patch("dvc.analytics._runtime_info", return_value={})
def test_main_analytics(mock_is_enabled, mock_daemon, mock_report, dvc_repo):
    assert 0 == main(["add", "foo"])
    assert mock_is_enabled.called
    assert mock_report.called
    assert mock_daemon.called
