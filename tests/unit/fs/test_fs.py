import pytest

from dvc.config import RemoteNotFoundError
from dvc.fs import (
    HDFSFileSystem,
    HTTPFileSystem,
    HTTPSFileSystem,
    LocalFileSystem,
    S3FileSystem,
    SSHFileSystem,
    get_fs_cls,
    get_fs_config,
)


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
