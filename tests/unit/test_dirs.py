from dvc.dirs import global_config_dir
from dvc.env import DVC_GLOBAL_CONFIG_DIR


def test_global_config_dir_respects_env_var(monkeypatch):
    path = "/some/random/path"
    monkeypatch.setenv(DVC_GLOBAL_CONFIG_DIR, path)
    assert global_config_dir() == path
