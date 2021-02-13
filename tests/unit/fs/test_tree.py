import pytest

from dvc.config import ConfigError
from dvc.fs import get_cloud_fs
from dvc.path_info import CloudURLInfo


def test_get_cloud_fs(tmp_dir, dvc):
    tmp_dir.add_remote(name="base", url="s3://bucket/path", default=False)
    tmp_dir.add_remote(name="first", url="remote://base/first", default=False)
    tmp_dir.add_remote(
        name="second", url="remote://first/second", default=False
    )

    base = CloudURLInfo("s3://bucket/path")
    first = base / "first"
    second = first / "second"

    assert get_cloud_fs(dvc, name="base").path_info == base
    assert get_cloud_fs(dvc, name="first").path_info == first
    assert get_cloud_fs(dvc, name="second").path_info == second


def test_get_cloud_fs_validate(tmp_dir, dvc):
    tmp_dir.add_remote(
        name="base", url="ssh://example.com/path", default=False
    )
    tmp_dir.add_remote(
        name="first",
        config={"url": "remote://base/first", "type": "symlink"},
        default=False,
    )
    tmp_dir.add_remote(
        name="second",
        config={"url": "remote://first/second", "oss_key_id": "mykey"},
        default=False,
    )

    assert get_cloud_fs(dvc, name="base").config == {
        "url": "ssh://example.com/path"
    }
    assert get_cloud_fs(dvc, name="first").config == {
        "url": "ssh://example.com/path/first",
        "type": ["symlink"],
    }

    with pytest.raises(ConfigError):
        get_cloud_fs(dvc, name="second")
