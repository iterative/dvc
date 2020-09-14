from dvc.path_info import CloudURLInfo
from dvc.tree import get_cloud_tree


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
