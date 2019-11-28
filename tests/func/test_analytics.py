import mock
import json

from dvc.main import main
from dvc.utils.compat import str


@mock.patch("dvc.analytics.send")
def test_daemon_analytics(mock_send, tmp_path):
    report = {"name": "dummy report"}
    fname = tmp_path / "report"
    fname.write_text(str(json.dumps(report)))

    assert 0 == main(["daemon", "analytics", str(fname)])
    assert mock_send.called
