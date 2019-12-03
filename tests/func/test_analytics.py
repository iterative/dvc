import mock

from dvc.main import main


@mock.patch("dvc.analytics.collect")
@mock.patch("dvc.analytics.send")
def test_daemon_analytics(mock_collect, mock_send, tmp_path):
    assert 0 == main(["daemon", "analytics", None, 0])
    assert mock_collect.called
    assert mock_send.called
