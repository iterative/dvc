import json
import platform

import pytest
from voluptuous import Any, Schema

from dvc import analytics
from dvc.cli import parse_args


@pytest.fixture
def tmp_global_dir(mocker, tmp_path):
    """
    Fixture to prevent modifying the actual global config
    """

    def _user_config_dir(appname, *_args, **_kwargs):
        return str(tmp_path / appname)

    mocker.patch("iterative_telemetry.user_config_dir", _user_config_dir)


def test_collect_and_send_report(mocker, tmp_global_dir):
    mock_json = mocker.patch("json.dump")
    mock_daemon = mocker.patch("dvc.daemon._spawn")
    analytics.collect_and_send_report()
    report = mock_json.call_args[0][0]

    assert not report.get("cmd_class")
    assert not report.get("cmd_return_code")

    args = parse_args(["add", "foo"])
    return_code = 0

    analytics.collect_and_send_report(args, return_code)
    report = mock_json.call_args[0][0]

    assert report["cmd_class"] == "CmdAdd"
    assert report["cmd_return_code"] == return_code

    assert mock_daemon.call_count == 2


def test_runtime_info(tmp_global_dir):
    schema = Schema(
        {
            "dvc_version": str,
            "is_binary": bool,
            "scm_class": Any("Git", None),
            "user_id": str,
            "system_info": dict,
        },
        required=True,
    )

    assert schema(analytics._runtime_info())


def test_send(mocker, tmp_path):
    mock_post = mocker.patch("requests.post")

    import requests

    url = "https://analytics.dvc.org"
    report = {"name": "dummy report"}
    report_file = tmp_path / "report"

    report_file.write_text(json.dumps(report))
    mock_post.side_effect = requests.exceptions.RequestException

    analytics.send(str(report_file))
    assert mock_post.called
    assert mock_post.call_args[0][0] == url
    assert not report_file.exists()


@pytest.mark.parametrize(
    "config, result",
    [
        ({}, True),
        ({"analytics": "false"}, False),
        ({"analytics": "true"}, True),
        ({"unknown": "broken"}, True),
        ({"analytics": "false", "unknown": "broken"}, False),
    ],
)
def test_is_enabled(dvc, config, result, monkeypatch, tmp_global_dir):
    with dvc.config.edit(validate=False) as conf:
        conf["core"] = config

    # reset DVC_TEST env var, which affects `is_enabled()`
    monkeypatch.delenv("DVC_TEST")
    monkeypatch.delenv("DVC_NO_ANALYTICS", raising=False)

    assert result == analytics.is_enabled()


@pytest.mark.parametrize(
    "config, env, result",
    [
        (None, None, True),
        (None, "true", False),
        (None, "false", False),  # only checking if env is set
        ("false", None, False),
        ("false", "true", False),
        ("false", "false", False),
        ("true", None, True),
        ("true", "true", False),
        ("true", "false", False),  # we checking if env is set
    ],
)
def test_is_enabled_env_neg(
    dvc, config, env, result, monkeypatch, tmp_global_dir
):
    # reset DVC_TEST env var, which affects `is_enabled()`
    monkeypatch.delenv("DVC_TEST")
    monkeypatch.delenv("DVC_NO_ANALYTICS", raising=False)

    with dvc.config.edit() as conf:
        conf["core"] = {}

    assert analytics.is_enabled()

    if config is not None:
        with dvc.config.edit() as conf:
            conf["core"] = {"analytics": config}

    if env is not None:
        monkeypatch.setenv("DVC_NO_ANALYTICS", env)

    assert result == analytics.is_enabled()


def test_system_info():
    schema = Schema({"os": Any("windows", "mac", "linux")}, required=True)

    system = platform.system()

    if system == "Windows":
        schema = schema.extend(
            {
                "windows_version_build": int,
                "windows_version_major": int,
                "windows_version_minor": int,
                "windows_version_service_pack": str,
            }
        )

    if system == "Darwin":
        schema = schema.extend({"mac_version": str})

    if system == "Linux":
        schema = schema.extend(
            {
                "linux_distro": str,
                "linux_distro_like": str,
                "linux_distro_version": str,
            }
        )

    assert schema(analytics._system_info())
