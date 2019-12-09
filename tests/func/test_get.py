from __future__ import unicode_literals

import filecmp
import logging
import os

import pytest

from dvc.config import Config
from dvc.exceptions import UrlNotDvcRepoError
from dvc.repo.get import GetDVCFileError, PathMissingError
from dvc.repo import Repo
from dvc.system import System
from dvc.utils import makedirs
from dvc.utils.compat import fspath
from dvc.utils import fspath_py35
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


# https://github.com/iterative/dvc/pull/2837#discussion_r352123053
def test_get_full_dvc_path(erepo):
    external_data_dir = erepo.mkdtemp()
    external_data = os.path.join(external_data_dir, "ext_data")
    with open(external_data, "w+") as fobj:
        fobj.write("ext_data")

    cur_dir = os.getcwd()
    os.chdir(erepo.root_dir)
    erepo.dvc.add(external_data)
    erepo.dvc.scm.add(["ext_data.dvc"])
    erepo.dvc.scm.commit("add external data")
    os.chdir(cur_dir)

    Repo.get(erepo.root_dir, external_data, "ext_data_imported")
    assert os.path.isfile("ext_data_imported")
    assert filecmp.cmp(external_data, "ext_data_imported", shallow=False)


def test_non_cached_output(tmp_path, erepo):
    os.chdir(erepo.root_dir)
    erepo.dvc.run(
        outs_no_cache=["non_cached_file"], cmd="echo hello > non_cached_file"
    )
    erepo.dvc.scm.add(["non_cached_file", "non_cached_file.dvc"])
    erepo.dvc.scm.commit("add non-cached output")
    os.chdir(fspath_py35(tmp_path))
    Repo.get(erepo.root_dir, "non_cached_file")

    src = os.path.join(erepo.root_dir, "non_cached_file")
    assert os.path.isfile("non_cached_file")
    assert filecmp.cmp(src, "non_cached_file", shallow=False)


# https://github.com/iterative/dvc/pull/2837#discussion_r352123053
def test_absolute_file_outside_repo(erepo):
    with pytest.raises(PathMissingError):
        Repo.get(erepo.root_dir, "/root/")


def test_unknown_path(erepo):
    with pytest.raises(PathMissingError):
        Repo.get(erepo.root_dir, "a_non_existing_file")


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
