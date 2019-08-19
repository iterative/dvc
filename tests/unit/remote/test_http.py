import pytest
from dvc.remote.http import RemoteHTTP
from dvc.remote.base import RemoteConfigError


def test_no_traverse_compatibility(dvc_repo):
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "no_traverse": False,
    }

    with pytest.raises(RemoteConfigError):
        RemoteHTTP(dvc_repo, config)
