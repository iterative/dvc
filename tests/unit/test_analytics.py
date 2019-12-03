import json
import pytest
import mock

from dvc import analytics
from dvc.utils.compat import builtin_str, str


@pytest.fixture
def tmp_global_config(tmp_path):
    """
    Fixture to prevent modifying the actual global config
    """
    with mock.patch(
        "dvc.config.Config.get_global_config_dir", return_value=tmp_path
    ):
        yield


def test_collect(tmp_global_config):
    report = analytics.collect(return_code=0)
    assert analytics.report_schema(report)


@mock.patch("requests.post")
def test_send(mock_post, tmp_path):
    url = "https://analytics.dvc.org"
    report = {"name": "dummy report"}
    fname = tmp_path / "report"

    fname.write_text(str(json.dumps(report)))
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
def test_is_enabled(dvc_repo, config, result, monkeypatch):
    configobj = dvc_repo.config._repo_config
    configobj["core"] = config
    configobj.write()

    # reset DVC_TEST env var, which affects `is_enabled()`
    monkeypatch.delenv("DVC_TEST")

    assert result == analytics.is_enabled()


def test_find_or_create_user_id(tmp_global_config):
    created = analytics.find_or_create_user_id()
    found = analytics.find_or_create_user_id()

    assert created == found
