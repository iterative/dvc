import pytest

from dvc.config import ConfigError
from dvc.fs import get_cloud_fs


def test_get_cloud_fs(tmp_dir, dvc):
    tmp_dir.add_remote(name="base", url="s3://bucket/path", default=False)
    tmp_dir.add_remote(name="first", url="remote://base/first", default=False)
    tmp_dir.add_remote(
        name="second", url="remote://first/second", default=False
    )

    base = "bucket/path"
    first = f"{base}/first"
    second = f"{first}/second"

    _, _, path = get_cloud_fs(dvc, name="base")
    assert path == base
    _, _, path = get_cloud_fs(dvc, name="first")
    assert path == first
    _, _, path = get_cloud_fs(dvc, name="second")
    assert path == second


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

    assert get_cloud_fs(dvc, name="base")[1]["host"] == "example.com"
    assert get_cloud_fs(dvc, name="first")[1]["host"] == "example.com"
    assert get_cloud_fs(dvc, name="first")[1]["type"] == ["symlink"]

    with pytest.raises(ConfigError):
        get_cloud_fs(dvc, name="second")
