from __future__ import unicode_literals

import os
import filecmp

import pytest

from dvc.exceptions import UrlNotDvcRepoError, GetDVCFileError
from dvc.repo import Repo
from dvc.utils import makedirs

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


def test_get_from_non_dvc_repo(git_erepo):
    with pytest.raises(UrlNotDvcRepoError):
        Repo.get(git_erepo.root_dir, "some_file.zip")


def test_get_a_dvc_file(repo_dir, erepo):
    with pytest.raises(GetDVCFileError):
        Repo.get(erepo.root_dir, "some_file.dvc")


@pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
def test_get_to_dir(dname, repo_dir, erepo):
    src = erepo.FOO

    makedirs(dname, exist_ok=True)

    Repo.get(erepo.root_dir, src, dname)

    dst = os.path.join(dname, os.path.basename(src))

    assert os.path.isdir(dname)
    assert filecmp.cmp(repo_dir.FOO, dst, shallow=False)
