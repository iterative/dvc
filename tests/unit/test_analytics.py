import pytest
import mock
import platform
import json

from voluptuous import Schema, Any

from dvc import analytics
from dvc.utils.compat import str, builtin_str


@pytest.fixture
def tmp_global_config(tmp_path):
    """
    Fixture to prevent modifying the actual global config
    """
    with mock.patch(
        "dvc.config.Config.get_global_config_dir", return_value=tmp_path
    ):
        yield


def test_runtime_info(tmp_global_config):
    schema = Schema(
        {
            "dvc_version": Any(builtin_str, str),
            "is_binary": bool,
            "scm_class": Any("Git", None),
            "user_id": Any(builtin_str, str),
            "system_info": dict,
        }
    )

    assert schema(analytics.runtime_info())


@mock.patch("requests.post")
def test_send(mock_post, tmp_path):
    url = "https://analytics.dvc.org"
    report = {"name": "dummy report"}
    fname = tmp_path / "report"

    with open(fname, "w") as fobj:
        json.dump(report, fobj)

    analytics.send(fname)
    assert mock_post.called
    assert mock_post.call_args.args[0] == url


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
def test_is_enabled(dvc_repo, config, result, monkeypatch, tmp_global_config):
    configobj = dvc_repo.config._repo_config
    configobj["core"] = config
    configobj.write()

    # reset DVC_TEST env var, which affects `is_enabled()`
    monkeypatch.delenv("DVC_TEST")

    assert result == analytics.is_enabled()


def test_system_info():
    schema = Schema({"os": Any("windows", "mac", "linux")})

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
                "linux_distro_like": Any(str, None),
                "linux_distro_version": Any(str, None),
            }
        )

    assert schema(analytics.system_info())


def test_find_or_create_user_id(tmp_global_config):
    created = analytics.find_or_create_user_id()
    found = analytics.find_or_create_user_id()

    assert created == found
