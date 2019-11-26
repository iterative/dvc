import pytest
import mock

from dvc import analytics


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


def test_find_or_create_user_id(tmp_path):
    with mock.patch(
        "dvc.config.Config.get_global_config_dir", return_value=tmp_path
    ):
        created = analytics.find_or_create_user_id()
        found = analytics.find_or_create_user_id()

    assert  created == found
