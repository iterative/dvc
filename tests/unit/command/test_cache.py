import os
import pytest
from mock import MagicMock

from dvc.config import Config


def test_path_replacement():

    config_filename = "./config/file"

    # Extract the "rel()" function from Config._save_paths
    Config._map_dirs = MagicMock()
    Config._save_paths(None, config_filename)
    _, rel_func = Config._map_dirs.call_args[0]

    assert rel_func("cache") == "../cache"
    assert rel_func("../cache") == "../../cache"
    assert rel_func("/path/to/cache") == "/path/to/cache"

    assert rel_func("..\\cache") == "../../cache"
    assert rel_func("c:\\path\\to\\cache") == "c:/path/to/cache"

    assert rel_func("ssh://something") == "ssh://something"


@pytest.mark.skipif(os.name != "nt", reason="Windows only")
def test_path_replacement_windows():

    config_filename = "./config/file"

    # Extract the "rel()" function from Config._save_paths
    Config._map_dirs = MagicMock()
    Config._save_paths(None, config_filename)
    _, rel_func = Config._map_dirs.call_args[0]

    assert rel_func("..\\cache") == "../../cache"
    assert rel_func("c:\\path\\to\\cache") == "c:/path/to/cache"
