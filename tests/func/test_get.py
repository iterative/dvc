from __future__ import unicode_literals

import filecmp
import logging
import os

import pytest

from dvc.config import Config
from dvc.exceptions import GetDVCFileError
from dvc.exceptions import UrlNotDvcRepoError
from dvc.exceptions import NoAbsolutePathError
from dvc.repo import Repo
from dvc.system import System
from dvc.utils import makedirs
from dvc.utils.compat import fspath
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


def test_get_regular_file(erepo):
    src = "some_file"
    dst = "some_file_imported"

    src_path = os.path.join(erepo.root_dir, src)
    erepo.create(src_path, "hello")
    erepo.dvc.scm.add([src_path])
    erepo.dvc.scm.commit("add a regular file")
    Repo.get(erepo.root_dir, src, dst)

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    assert filecmp.cmp(src_path, dst, shallow=False)


def test_get_regular_dir(erepo):
    src = "some_directory"
    dst = "some_directory_imported"

    src_file_path = os.path.join(erepo.root_dir, src, "file.txt")
    erepo.create(src_file_path, "hello")
    erepo.dvc.scm.add([src_file_path])
    erepo.dvc.scm.commit("add a regular dir")
    Repo.get(erepo.root_dir, src, dst)

    assert os.path.exists(dst)
    assert os.path.isdir(dst)
    trees_equal(os.path.join(erepo.root_dir, src), dst)


def test_cache_type_is_properly_overridden(repo_dir, erepo):
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


# Prevent `Repo.get()` from copying any file on the filesystem
# https://github.com/iterative/dvc/pull/2837#discussion_r352123053
def test_fails_with_absolute_paths(erepo):
    with pytest.raises(NoAbsolutePathError):
        Repo.get(erepo.root_dir, "/root/")


@pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
def test_get_to_dir(dname, erepo):
    src = erepo.FOO

    makedirs(dname, exist_ok=True)

    Repo.get(erepo.root_dir, src, dname)

    dst = os.path.join(dname, os.path.basename(src))

    assert os.path.isdir(dname)
    assert filecmp.cmp(erepo.FOO, dst, shallow=False)


def test_get_from_non_dvc_master(erepo, tmp_path, monkeypatch, caplog):
    monkeypatch.chdir(fspath(tmp_path))
    erepo.dvc.scm.repo.index.remove([".dvc"], r=True)
    erepo.dvc.scm.commit("remove .dvc")

    caplog.clear()
    imported_file = "foo_imported"
    with caplog.at_level(logging.INFO, logger="dvc"):
        Repo.get(erepo._root_dir, erepo.FOO, out=imported_file, rev="branch")

    assert caplog.text == ""
    assert filecmp.cmp(
        os.path.join(erepo._root_dir, erepo.FOO), imported_file, shallow=False
    )
