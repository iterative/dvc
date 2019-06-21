import os

from dvc.system import System


def test_destroy(repo_dir, dvc_repo):
    # NOTE: using symlink to ensure that data was unprotected after `destroy`
    dvc_repo.config.set("cache", "type", "symlink")

    foo_stage, = dvc_repo.add(repo_dir.FOO)
    data_dir_stage, = dvc_repo.add(repo_dir.DATA_DIR)

    dvc_repo.destroy()

    assert not os.path.exists(dvc_repo.dvc_dir)
    assert not os.path.exists(foo_stage.path)
    assert not os.path.exists(data_dir_stage.path)

    assert os.path.isfile(repo_dir.FOO)
    assert os.path.isdir(repo_dir.DATA_DIR)
    assert os.path.isfile(repo_dir.DATA)
    assert os.path.isdir(repo_dir.DATA_SUB_DIR)
    assert os.path.isfile(repo_dir.DATA_SUB)

    assert not System.is_symlink(repo_dir.FOO)
    assert not System.is_symlink(repo_dir.DATA_DIR)
    assert not System.is_symlink(repo_dir.DATA)
    assert not System.is_symlink(repo_dir.DATA_SUB)
