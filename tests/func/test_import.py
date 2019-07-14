import os
import filecmp

from tests.utils import trees_equal


def test_import(repo_dir, git, dvc_repo, erepo):
    src = erepo.FOO
    dst = erepo.FOO + "_imported"

    dvc_repo.imp(erepo.root_dir, src, dst)

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    assert filecmp.cmp(repo_dir.FOO, dst, shallow=False)
    assert git.git.check_ignore(dst)


def test_import_dir(repo_dir, git, dvc_repo, erepo):
    src = erepo.DATA_DIR
    dst = erepo.DATA_DIR + "_imported"

    dvc_repo.imp(erepo.root_dir, src, dst)

    assert os.path.exists(dst)
    assert os.path.isdir(dst)
    trees_equal(src, dst)
    assert git.git.check_ignore(dst)


def test_import_rev(repo_dir, git, dvc_repo, erepo):
    src = "version"
    dst = src

    dvc_repo.imp(erepo.root_dir, src, dst, rev="branch")

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    with open(dst, "r+") as fobj:
        assert fobj.read() == "branch"
    assert git.git.check_ignore(dst)
