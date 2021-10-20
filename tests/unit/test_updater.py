import json
import logging
import os
import time

import pytest

from dvc import __version__
from dvc.updater import Updater
from tests.func.parsing.test_errors import escape_ansi


@pytest.fixture
def tmp_global_dir(mocker, tmp_path):
    """
    Fixture to prevent modifying the actual global config
    """
    mocker.patch("dvc.config.Config.get_dir", return_value=str(tmp_path))


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.setenv("DVC_TEST", "False")


@pytest.fixture
def updater(tmp_path, tmp_global_dir):
    return Updater(tmp_path)


@pytest.fixture
def mock_tty(mocker):
    return mocker.patch("sys.stdout.isatty", return_value=True)


def test_fetch(mocker, updater):
    mock_get = mocker.patch("requests.get")
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"version": __version__}

    assert not os.path.exists(updater.updater_file)

    updater.fetch(detach=False)

    mock_get.assert_called_once_with(Updater.URL, timeout=Updater.TIMEOUT_GET)
    assert os.path.isfile(updater.updater_file)

    with open(updater.updater_file, encoding="utf-8") as fobj:
        info = json.load(fobj)

    assert info["version"] == __version__


@pytest.mark.parametrize(
    "config, result",
    [
        ({}, True),
        ({"check_update": "true"}, True),
        ({"check_update": "false"}, False),
    ],
)
def test_is_enabled(dvc, updater, config, result):
    with dvc.config.edit(validate=False) as conf:
        conf["core"] = config

    assert result == updater.is_enabled()


@pytest.mark.parametrize("result", [True, False])
def test_check_update_respect_config(result, updater, mocker):
    mock_check = mocker.patch("dvc.updater.Updater._check")
    mocker.patch.object(updater, "is_enabled", return_value=result)
    updater.check()
    assert result == mock_check.called


@pytest.mark.parametrize(
    "current,latest,notify",
    [
        ("0.0.2", "0.0.2", False),
        ("0.0.2", "0.0.3", True),
        ("0.0.2", "0.0.1", False),
    ],
    ids=["uptodate", "behind", "ahead"],
)
def test_check_updates(mocker, capsys, updater, current, latest, notify):
    mocker.patch("sys.stdout.isatty", return_value=True)

    updater.current = current
    with open(updater.updater_file, "w+", encoding="utf-8") as f:
        json.dump({"version": latest}, f)

    updater.check()
    out, err = capsys.readouterr()
    expected_message = (
        (
            f"You are using dvc version {current}; "
            f"however, version {latest} is available.\n"
        )
        if notify
        else ""
    )

    assert expected_message in escape_ansi(err)
    assert not out


def test_check_refetches_each_day(mock_tty, updater, caplog, mocker):
    updater.current = "0.0.8"
    with open(updater.updater_file, "w+", encoding="utf-8") as f:
        json.dump({"version": "0.0.9"}, f)
    fetch = mocker.patch.object(updater, "fetch")

    time_value = time.time() + 24 * 60 * 60 + 10
    mock_time = mocker.patch("time.time", return_value=time_value)

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="dvc.updater"):
        updater.check()
    assert not caplog.text
    fetch.assert_called_once()
    mock_time.assert_called()


def test_check_fetches_on_invalid_data_format(
    mock_tty, updater, caplog, mocker
):
    updater.current = "0.0.5"
    with open(updater.updater_file, "w+", encoding="utf-8") as f:
        f.write('"{"version: "0.0.6"')
    fetch = mocker.patch.object(updater, "fetch")
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="dvc.updater"):
        updater.check()
    assert not caplog.text
    fetch.assert_called_once()


def test_check(mocker, updater):
    mock_check = mocker.patch("dvc.updater.Updater._check")
    updater.check()
    updater.check()
    updater.check()

    assert mock_check.call_count == 3


@pytest.mark.parametrize(
    "pkg, instruction",
    [
        ("pip", "To upgrade, run 'pip install --upgrade dvc'."),
        ("rpm", "To upgrade, run 'yum update dvc'."),
        ("brew", "To upgrade, run 'brew upgrade dvc'."),
        ("deb", "To upgrade, run 'apt-get install --only-upgrade dvc'."),
        ("conda", "To upgrade, run 'conda update dvc'."),
        ("choco", "To upgrade, run 'choco upgrade dvc'."),
        (
            "osxpkg",
            "To upgrade, uninstall dvc and reinstall from https://dvc.org.",
        ),
        (
            "exe",
            "To upgrade, uninstall dvc and reinstall from https://dvc.org.",
        ),
        (
            "binary",
            "To upgrade, uninstall dvc and reinstall from https://dvc.org.",
        ),
        (
            None,
            "Find the latest release at "
            "https://github.com/iterative/dvc/releases/latest.",
        ),
        (
            "unknown",
            "Find the latest release at "
            "https://github.com/iterative/dvc/releases/latest.",
        ),
    ],
)
def test_notify_message(updater, pkg, instruction):
    update_message = (
        "You are using dvc version 0.0.2; however, version 0.0.3 is available."
    )

    message = updater._get_message("0.0.3", current="0.0.2", pkg=pkg)
    assert message.plain.splitlines() == ["", update_message, instruction]
