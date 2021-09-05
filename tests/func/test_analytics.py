import os
from unittest.mock import call

import pytest

from dvc.analytics import _scm_in_use, collect_and_send_report
from dvc.main import main
from dvc.repo import Repo


def test_daemon_analytics(mocker, tmp_path):
    mock_send = mocker.patch("dvc.analytics.send")
    report = os.fspath(tmp_path)
    assert 0 == main(["daemon", "analytics", report])

    mock_send.assert_called_with(report)


def test_main_analytics(mocker, tmp_dir, dvc):
    mock_is_enabled = mocker.patch("dvc.analytics.collect_and_send_report")
    mock_report = mocker.patch("dvc.analytics.is_enabled", return_value=True)
    tmp_dir.gen("foo", "text")
    assert 0 == main(["add", "foo"])
    assert mock_is_enabled.called
    assert mock_report.called


@pytest.fixture
def mock_daemon(mocker):
    def func(argv):
        return main(["daemon", *argv])

    m = mocker.patch("dvc.daemon.daemon", mocker.MagicMock(side_effect=func))
    yield m


class ANY:
    def __init__(self, expected_type):
        self.expected_type = expected_type

    def __repr__(self):
        return "Any" + self.expected_type.__name__.capitalize()

    def __eq__(self, other):
        return isinstance(other, self.expected_type)


def test_collect_and_send_report(mocker, dvc, mock_daemon):
    mock_post = mocker.patch("requests.post")
    collect_and_send_report()

    assert mock_daemon.call_count == 1
    assert mock_post.call_count == 1
    assert mock_post.call_args == call(
        "https://analytics.dvc.org",
        json=ANY(dict),
        headers={"content-type": "application/json"},
        timeout=5,
    )


def test_scm_dvc_only(tmp_dir, dvc):
    scm = _scm_in_use()
    assert scm == "NoSCM"


def test_scm_git(tmp_dir, scm, dvc):
    scm = _scm_in_use()
    assert scm == "Git"


def test_scm_subrepo(tmp_dir, scm):
    subdir = tmp_dir / "subdir"
    subdir.mkdir()

    with subdir.chdir():
        Repo.init(subdir=True)
        scm = _scm_in_use()

    assert scm == "Git"
