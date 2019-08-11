import os
import filecmp

from dvc.repo import Repo


def test_get_file(repo_dir):
    src = repo_dir.FOO
    dst = repo_dir.FOO + "_imported"

    Repo.get_url(src, dst)

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    assert filecmp.cmp(repo_dir.FOO, dst, shallow=False)
