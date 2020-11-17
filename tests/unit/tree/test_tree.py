import os
import stat

import pytest

from dvc.config import ConfigError
from dvc.path_info import CloudURLInfo, PathInfo
from dvc.tree import LocalTree, get_cloud_tree


def test_get_cloud_tree(tmp_dir, dvc):
    tmp_dir.add_remote(name="base", url="s3://bucket/path", default=False)
    tmp_dir.add_remote(name="first", url="remote://base/first", default=False)
    tmp_dir.add_remote(
        name="second", url="remote://first/second", default=False
    )

    base = CloudURLInfo("s3://bucket/path")
    first = base / "first"
    second = first / "second"

    assert get_cloud_tree(dvc, name="base").path_info == base
    assert get_cloud_tree(dvc, name="first").path_info == first
    assert get_cloud_tree(dvc, name="second").path_info == second


def test_get_cloud_tree_validate(tmp_dir, dvc):
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

    assert get_cloud_tree(dvc, name="base").config == {
        "url": "ssh://example.com/path"
    }
    assert get_cloud_tree(dvc, name="first").config == {
        "url": "ssh://example.com/path/first",
        "type": ["symlink"],
    }

    with pytest.raises(ConfigError):
        get_cloud_tree(dvc, name="second")


@pytest.mark.skipif(os.name == "nt", reason="Not supported for Windows.")
@pytest.mark.parametrize(
    "mode, expected", [(None, LocalTree.CACHE_MODE), (0o777, 0o777)],
)
def test_upload_file_mode(tmp_dir, mode, expected):
    tmp_dir.gen("src", "foo")
    src = PathInfo(tmp_dir / "src")
    dest = PathInfo(tmp_dir / "dest")
    tree = LocalTree(None, {"url": os.fspath(tmp_dir)})
    tree.upload(src, dest, file_mode=mode)
    assert (tmp_dir / "dest").exists()
    assert (tmp_dir / "dest").read_text() == "foo"
    assert stat.S_IMODE(os.stat(dest).st_mode) == expected
