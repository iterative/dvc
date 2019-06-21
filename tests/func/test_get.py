import os
import filecmp

from dvc.repo import Repo

from tests.utils import trees_equal


def test_get_repo_file(repo_dir, erepo):
    src = erepo.FOO
    dst = erepo.FOO + "_imported"

    Repo.get(erepo.root_dir, src, dst)

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    assert filecmp.cmp(repo_dir.FOO, dst, shallow=False)


def test_get_repo_dir(repo_dir, erepo):
    src = erepo.DATA_DIR
    dst = erepo.DATA_DIR + "_imported"

    Repo.get(erepo.root_dir, src, dst)

    assert os.path.exists(dst)
    assert os.path.isdir(dst)
    trees_equal(src, dst)


def test_get_repo_rev(repo_dir, erepo):
    src = "version"
    dst = src

    Repo.get(erepo.root_dir, src, dst, rev="branch")

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    with open(dst, "r+") as fobj:
        assert fobj.read() == "branch"
