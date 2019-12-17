import json
import os
import mock
import pytest

from dvc import __version__
from dvc.updater import Updater


@pytest.fixture
def updater(dvc):
    return Updater(dvc.dvc_dir)


@mock.patch("requests.get")
def test_fetch(mock_get, updater):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"version": __version__}

    assert not os.path.exists(updater.updater_file)

    updater.fetch(detach=False)

    mock_get.assert_called_once_with(Updater.URL, timeout=Updater.TIMEOUT_GET)
    assert os.path.isfile(updater.updater_file)

    with open(updater.updater_file, "r") as fobj:
        info = json.load(fobj)

    assert info["version"] == __version__


@pytest.mark.parametrize(
    "latest, current, result",
    [
        ("0.20.8", "0.21.0", False),
        ("0.20.8", "0.20.8", False),
        ("0.20.8", "0.19.0", True),
    ],
)
def test_is_outdated(latest, current, result, updater):
    updater.latest = latest
    updater.current = current

    assert updater._is_outdated() == result


@pytest.mark.skipif(
    os.getenv("TRAVIS_EVENT_TYPE") != "cron",
    reason="Only run on travis CRON to avoid generating too much logs",
)
@mock.patch("dvc.updater.Updater._check")
def test_check(mock_check, updater, monkeypatch):
    monkeypatch.delenv("CI", None)
    monkeypatch.setenv("DVC_TEST", False)

    updater.check()
    updater.check()
    updater.check()

    assert mock_check.call_count == 3
