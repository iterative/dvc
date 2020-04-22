import os
import pytest

from dvc.config import Config


@pytest.mark.skipif(os.name == "nt", reason="Linux only")
def test_path_replacement():

    config_dirname = "./config"

    assert Config._to_relpath(config_dirname, "cache") == "../cache"
    assert Config._to_relpath(config_dirname, "../cache") == "../../cache"
    assert (
        Config._to_relpath(config_dirname, "/path/to/cache")
        == "/path/to/cache"
    )

    assert (
        Config._to_relpath(config_dirname, "ssh://something")
        == "ssh://something"
    )


@pytest.mark.skipif(os.name != "nt", reason="Windows only")
def test_path_replacement_windows():

    config_dirname = ".\\config"

    assert Config._to_relpath(config_dirname, "..\\cache") == "../../cache"
    assert (
        Config._to_relpath(config_dirname, "c:\\path\\to\\cache")
        == "c:/path/to/cache"
    )

    assert (
        Config._to_relpath(config_dirname, "ssh://something")
        == "ssh://something"
    )
