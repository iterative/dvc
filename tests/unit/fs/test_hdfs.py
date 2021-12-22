import pytest

from dvc.fs import HDFSFileSystem


@pytest.mark.parametrize(
    "fs_args, expected_url",
    [
        ({"host": "example.com"}, "hdfs://example.com"),
        ({"host": "example.com", "port": None}, "hdfs://example.com"),
        ({"host": "example.com", "port": 8020}, "hdfs://example.com:8020"),
    ],
)
def test_hdfs_unstrip_protocol(fs_args, expected_url):
    fs = HDFSFileSystem(**fs_args)
    assert fs.unstrip_protocol("/path") == expected_url + "/path"
