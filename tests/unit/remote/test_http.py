import pytest

from dvc.config import ConfigError
from dvc.remote.http import RemoteHTTP


def test_no_traverse_compatibility(dvc_repo):
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "no_traverse": False,
    }

    with pytest.raises(ConfigError):
        RemoteHTTP(dvc_repo, config)
