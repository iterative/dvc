import pytest
from dvc_http import HTTPFileSystem, HTTPSFileSystem
from dvc_s3 import S3FileSystem
from dvc_ssh import SSHFileSystem

from dvc.config import RemoteNotFoundError
from dvc.fs import LocalFileSystem, get_fs_cls, get_fs_config

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
    with pytest.raises(RemoteNotFoundError):
        get_fs_config(None, {"remote": {}}, name="myremote")
