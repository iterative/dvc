import sys

import pytest

from dvc.dirs import global_config_dir, site_cache_dir
from dvc.env import DVC_GLOBAL_CONFIG_DIR, DVC_SITE_CACHE_DIR


def test_global_config_dir_respects_env_var(monkeypatch):
    path = "/some/random/path"
    monkeypatch.setenv(DVC_GLOBAL_CONFIG_DIR, path)
    assert global_config_dir() == path


@pytest.mark.skipif(sys.platform != "linux", reason="Only for Unix platforms")
def test_site_cache_dir_on_unix(monkeypatch):
    monkeypatch.delenv(DVC_SITE_CACHE_DIR, raising=False)
    assert site_cache_dir() == "/var/tmp/dvc"
