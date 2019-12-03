import mock

from dvc.main import main


@mock.patch("dvc.analytics.send")
def test_daemon_analytics(mock_send, tmp_path):
    assert 0 == main(["daemon", "analytics", None, 0])
    assert mock_send.called
