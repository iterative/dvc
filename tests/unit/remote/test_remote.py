import pytest

from dvc.tree import get_cloud_tree
from dvc.tree.gs import GSTree
from dvc.tree.s3 import S3Tree


def test_remote_with_hash_jobs(dvc):
    dvc.config["remote"]["with_hash_jobs"] = {
        "url": "s3://bucket/name",
        "hash_jobs": 100,
    }
    dvc.config["core"]["hash_jobs"] = 200

    tree = get_cloud_tree(dvc, name="with_hash_jobs")
    assert tree.hash_jobs == 100


def test_remote_without_hash_jobs(dvc):
    dvc.config["remote"]["without_hash_jobs"] = {"url": "s3://bucket/name"}
    dvc.config["core"]["hash_jobs"] = 200

    tree = get_cloud_tree(dvc, name="without_hash_jobs")
    assert tree.hash_jobs == 200


def test_remote_without_hash_jobs_default(dvc):
    dvc.config["remote"]["without_hash_jobs"] = {"url": "s3://bucket/name"}

    tree = get_cloud_tree(dvc, name="without_hash_jobs")
    assert tree.hash_jobs == tree.HASH_JOBS


@pytest.mark.parametrize("tree_cls", [GSTree, S3Tree])
def test_makedirs_not_create_for_top_level_path(tree_cls, dvc, mocker):
    url = f"{tree_cls.scheme}://bucket/"
    tree = tree_cls(dvc, {"url": url})
    mocked_client = mocker.PropertyMock()
    # we use remote clients with same name as scheme to interact with remote
    mocker.patch.object(tree_cls, tree.scheme, mocked_client)

    tree.makedirs(tree.path_info)
    assert not mocked_client.called
