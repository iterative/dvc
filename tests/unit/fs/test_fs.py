import pytest
from dvc_hdfs import HDFSFileSystem
from dvc_http import HTTPFileSystem, HTTPSFileSystem
from dvc_s3 import S3FileSystem
from dvc_ssh import SSHFileSystem

from dvc.config import RemoteNotFoundError
from dvc.fs import LocalFileSystem, get_fs_cls, get_fs_config


@pytest.mark.parametrize(
    "url, cls",
    [
        ("s3://bucket/path", S3FileSystem),
        ("ssh://example.com:/dir/path", SSHFileSystem),
        ("hdfs://example.com/dir/path", HDFSFileSystem),
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
    ],
)
def test_get_fs_cls(url, cls):
    assert get_fs_cls({"url": url}) == cls


def test_get_fs_config():
    with pytest.raises(RemoteNotFoundError):
        get_fs_config(None, {"remote": {}}, name="myremote")
