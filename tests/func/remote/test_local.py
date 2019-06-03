import os
import shutil

from dvc.main import main
from dvc.remote.local import RemoteLOCAL
from dvc.repo import Repo
from dvc.utils import file_md5
from dvc.utils.fs import get_inode
from tests.utils import trees_equal


def test_should_create_cache_status_check_dir_on_add(dvc_repo, repo_dir):

    stages = dvc_repo.add(repo_dir.DATA_DIR)
    verification_dir = (
        stages[0].outs[0].cache_path
        + "."
        + RemoteLOCAL.STATUS_VERIFICATION_DIR_SUFFIX
    )

    assert os.path.exists(verification_dir)

    trees_equal(repo_dir.DATA_DIR, verification_dir)
    data_cache_path = dvc_repo.cache.local.get(file_md5(repo_dir.DATA)[0])
    data_sub_cache_path = dvc_repo.cache.local.get(
        file_md5(repo_dir.DATA_SUB)[0]
    )
    assert get_inode(os.path.join(verification_dir, "data")) == get_inode(
        data_cache_path
    )
    assert get_inode(
        os.path.join(verification_dir, "data_sub_dir", "data_sub")
    ) == get_inode(data_sub_cache_path)


def test_should_create_cache_status_check_dir_on_checkout(dvc_repo, repo_dir):
    local_storage = repo_dir.mkdtemp()
    ret = main(["remote", "add", "-d", "storage", local_storage])
    assert ret == 0

    dvc_repo = Repo(dvc_repo.dvc_dir)

    stages = dvc_repo.add(repo_dir.DATA_DIR)
    verification_dir = (
        stages[0].outs[0].cache_path
        + "."
        + RemoteLOCAL.STATUS_VERIFICATION_DIR_SUFFIX
    )

    assert dvc_repo.push() == 1

    shutil.rmtree(dvc_repo.cache.local.cache_dir)
    shutil.rmtree(repo_dir.DATA_DIR)
    assert not os.path.exists(verification_dir)

    dvc_repo.pull("{}.dvc".format(repo_dir.DATA_DIR))

    assert os.path.exists(verification_dir)
    trees_equal(repo_dir.DATA_DIR, verification_dir)
