import pytest

from dvc.analytics import Analytics


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

    assert result == Analytics.is_enabled()
