import pytest
from dvc_http import HTTPFileSystem, HTTPSFileSystem
from dvc_s3 import S3FileSystem
from dvc_ssh import SSHFileSystem

from dvc.config import RemoteNotFoundError
from dvc.fs import LocalFileSystem, get_cloud_fs, get_fs_cls, get_fs_config

url_cls_pairs = [
    ("s3://bucket/path", S3FileSystem),
    ("ssh://example.com:/dir/path", SSHFileSystem),
    ("http://example.com/path/to/file", HTTPFileSystem),
    ("https://example.com/path/to/file", HTTPSFileSystem),
    ("path/to/file", LocalFileSystem),
    ("path\\to\\file", LocalFileSystem),
    ("file", LocalFileSystem),
    ("./file", LocalFileSystem),
    (".\\file", LocalFileSystem),
    ("../file", LocalFileSystem),
    ("..\\file", LocalFileSystem),
    ("unknown://path", LocalFileSystem),
]


try:
    from dvc_hdfs import HDFSFileSystem

    url_cls_pairs += [("hdfs://example.com/dir/path", HDFSFileSystem)]
except ImportError:
    pass


@pytest.mark.parametrize("url, cls", url_cls_pairs)
def test_get_fs_cls(url, cls):
    assert get_fs_cls({"url": url}) == cls


def test_get_fs_config():
    result = get_fs_config({}, url="ssh://example.com:/dir/path")
    assert result == {"url": "ssh://example.com:/dir/path"}


def test_get_fs_config_error():
    with pytest.raises(RemoteNotFoundError):
        get_fs_config({"remote": {}}, name="myremote")


def test_remote_url():
    config = {
        "remote": {
            "base": {"url": "http://example.com"},
            "r1": {"url": "remote://base/r1", "user": "user"},
            "r2": {"url": "remote://r1/r2", "password": "123"},
        }
    }
    result = get_fs_config(config, url="remote://r2/foo")
    assert result == {
        "password": "123",
        "user": "user",
        "url": "http://example.com/r1/r2/foo",
    }


def test_get_cloud_fs():
    cls, config, path = get_cloud_fs({}, url="ssh://example.com:/dir/path")
    assert cls is SSHFileSystem
    assert config == {"host": "example.com", "verify": False}
    assert path == "/dir/path"
