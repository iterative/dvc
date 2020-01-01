import mock

from dvc.main import main
from dvc.compat import fspath


@mock.patch("dvc.analytics.send")
def test_daemon_analytics(mock_send, tmp_path):
    report = fspath(tmp_path)
    assert 0 == main(["daemon", "analytics", report])

    mock_send.assert_called_with(report)


@mock.patch("dvc.analytics.collect_and_send_report")
@mock.patch("dvc.analytics.is_enabled", return_value=True)
def test_main_analytics(mock_is_enabled, mock_report, tmp_dir, dvc):
    tmp_dir.gen("foo", "text")
    assert 0 == main(["add", "foo"])
    assert mock_is_enabled.called
    assert mock_report.called
