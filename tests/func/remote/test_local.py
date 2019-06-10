import os

import mock

import dvc
from dvc.main import main
from dvc.remote.local import RemoteLOCAL


def test_should_not_fail_add_on_unpacked_dir_creation_exception(
    dvc_repo, repo_dir
):

    with mock.patch.object(
        dvc.remote.local.RemoteLOCAL,
        "_create_unpacked_dir",
        side_effect=Exception,
    ):
        ret = main(["add", repo_dir.DATA_DIR])
    assert ret == 0


def test_should_create_unpacked_dir_on_status_check(dvc_repo, repo_dir):
    stages = dvc_repo.add(repo_dir.DATA_DIR)
    assert len(stages) == 1

    unpacked_dir = RemoteLOCAL._append_unpacked_suffix(
        stages[0].outs[0].cache_path
    )

    assert not os.path.exists(unpacked_dir)

    assert dvc_repo.status("{}.dvc".format(repo_dir.DATA_DIR)) == {}
    assert os.path.exists(unpacked_dir)
