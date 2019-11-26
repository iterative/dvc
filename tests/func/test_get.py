from __future__ import unicode_literals

import filecmp
import os

import pytest

from dvc.config import Config
from dvc.exceptions import GetDVCFileError
from dvc.exceptions import UrlNotDvcRepoError
from dvc.remote import RemoteConfig
from dvc.repo import Repo
from dvc.system import System
from dvc.utils import makedirs
from tests.utils import trees_equal


def test_get_repo_file(erepo):
    src = erepo.FOO
    dst = erepo.FOO + "_imported"

    Repo.get(erepo.root_dir, src, dst)

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    assert filecmp.cmp(erepo.FOO, dst, shallow=False)


def test_get_repo_dir(erepo):
    src = erepo.DATA_DIR
    dst = erepo.DATA_DIR + "_imported"

    Repo.get(erepo.root_dir, src, dst)

    assert os.path.exists(dst)
    assert os.path.isdir(dst)
    trees_equal(src, dst)


def test_cache_type_is_properly_overridden(erepo):
    erepo.dvc.config.set(
        Config.SECTION_CACHE, Config.SECTION_CACHE_TYPE, "symlink"
    )
    erepo.dvc.scm.add([erepo.dvc.config.config_file])
    erepo.dvc.scm.commit("set cache type to symlinks")

    src = erepo.FOO
    dst = erepo.FOO + "_imported"

    Repo.get(erepo.root_dir, src, dst)

    assert not System.is_symlink(dst)
    assert os.path.exists(dst)
    assert os.path.isfile(dst)


def test_get_repo_rev(erepo):
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


def test_get_a_dvc_file(erepo):
    with pytest.raises(GetDVCFileError):
        Repo.get(erepo.root_dir, "some_file.dvc")


@pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
def test_get_to_dir(dname, erepo):
    src = erepo.FOO

    makedirs(dname, exist_ok=True)

    Repo.get(erepo.root_dir, src, dname)

    dst = os.path.join(dname, os.path.basename(src))

    assert os.path.isdir(dname)
    assert filecmp.cmp(erepo.FOO, dst, shallow=False)


@pytest.fixture
def erepo_no_dvc_master(git_erepo):
    dvc_branch = "dvc_test"
    git_erepo.git.git.checkout("master", b=dvc_branch)
    git_erepo.dvc_branch = dvc_branch

    dvc_repo = Repo.init(git_erepo._root_dir)
    stage, = dvc_repo.add([git_erepo.FOO])
    dvc_repo.scm.add([".dvc", stage.relpath])

    rconfig = RemoteConfig(dvc_repo.config)
    rconfig.add("upstream", dvc_repo.cache.local.cache_dir, default=True)
    dvc_repo.scm.add([dvc_repo.config.config_file])

    dvc_repo.scm.commit("dvc branch initial")

    git_erepo.git.git.checkout("master")
    os.chdir(git_erepo._saved_dir)
    yield git_erepo


def test_get_from_non_dvc_master(empty_dir, erepo_no_dvc_master):
    Repo.get(
        erepo_no_dvc_master._root_dir,
        "foo",
        out="foo",
        rev=erepo_no_dvc_master.dvc_branch,
    )
