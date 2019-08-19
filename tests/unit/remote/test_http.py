import pytest
from dvc.remote.http import RemoteHTTP


def test_no_traverse_compatibility(dvc_repo):
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "no_traverse": False,
    }

    remote = RemoteHTTP(dvc_repo, config)

    with pytest.raises(NotImplementedError):
        remote.cache_exists(checksums=["12345678"])
