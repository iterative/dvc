import logging
import os

import pytest

from dvc.cache import Cache
from dvc.config import Config
from dvc.exceptions import UrlNotDvcRepoError
from dvc.repo.get import GetDVCFileError, PathMissingError
from dvc.repo import Repo
from dvc.system import System
from dvc.utils import makedirs
from dvc.utils.compat import fspath
from tests.utils import trees_equal


def test_get_repo_file(tmp_dir, erepo_dir, monkeypatch):
    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.dvc_gen("file", "contents", commit="create file")

    Repo.get(fspath(erepo_dir), "file", "file_imported")

    assert os.path.isfile("file_imported")
    assert (tmp_dir / "file_imported").read_text() == "contents"


def test_get_repo_dir(tmp_dir, erepo_dir, monkeypatch):
    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.dvc_gen({"dir": {"file": "contents"}}, commit="create dir")

    Repo.get(fspath(erepo_dir), "dir", "dir_imported")

    assert os.path.isdir("dir_imported")
    trees_equal(fspath(erepo_dir / "dir"), "dir_imported")


def test_get_git_file(tmp_dir, erepo_dir):
    src = "some_file"
    dst = "some_file_imported"

    erepo_dir.scm_gen({src: "hello"}, commit="add a regular file")

    Repo.get(fspath(erepo_dir), src, dst)

    assert (tmp_dir / dst).is_file()
    assert (tmp_dir / dst).read_text() == "hello"


def test_get_git_dir(tmp_dir, erepo_dir):
    src = "some_directory"
    dst = "some_directory_imported"

    erepo_dir.scm_gen({src: {"file.txt": "hello"}}, commit="add a regular dir")

    Repo.get(fspath(erepo_dir), src, dst)

    assert (tmp_dir / dst).is_dir()
    trees_equal(fspath(erepo_dir / src), fspath(tmp_dir / dst))


def test_cache_type_is_properly_overridden(tmp_dir, erepo_dir, monkeypatch):
    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.dvc.config.set(
            Config.SECTION_CACHE, Config.SECTION_CACHE_TYPE, "symlink"
        )
        erepo_dir.dvc.cache = Cache(erepo_dir.dvc)
        erepo_dir.scm_add(
            [erepo_dir.dvc.config.config_file], "set cache type to symlinks"
        )
        erepo_dir.dvc_gen("file", "contents", "create file")
    assert System.is_symlink(erepo_dir / "file")

    Repo.get(fspath(erepo_dir), "file", "file_imported")

    assert not System.is_symlink("file_imported")
    assert (tmp_dir / "file_imported").read_text() == "contents"


def test_get_repo_rev(tmp_dir, erepo_dir, monkeypatch):
    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.scm.checkout("new_branch", create_new=True)
        erepo_dir.dvc_gen("file", "contents", commit="create file on branch")
        erepo_dir.scm.checkout("master")

    Repo.get(fspath(erepo_dir), "file", "file_imported", rev="new_branch")

    assert (tmp_dir / "file_imported").read_text() == "contents"


def test_get_from_non_dvc_repo(tmp_dir, erepo_dir):
    erepo_dir.scm.repo.index.remove([erepo_dir.dvc.dvc_dir], r=True)
    erepo_dir.scm.commit("remove dvc")

    with pytest.raises(UrlNotDvcRepoError):
        Repo.get(fspath(erepo_dir), "some_file.zip")


def test_get_a_dvc_file(tmp_dir, erepo_dir):
    with pytest.raises(GetDVCFileError):
        Repo.get(fspath(erepo_dir), "some_file.dvc")


# https://github.com/iterative/dvc/pull/2837#discussion_r352123053
def test_get_full_dvc_path(tmp_dir, erepo_dir, tmp_path_factory, monkeypatch):
    path = tmp_path_factory.mktemp("ext")
    external_data = path / "ext_data"
    external_data.write_text("ext_data")

    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.dvc.add(fspath(external_data))
        erepo_dir.scm_add(["ext_data.dvc"], commit="add external data")

    Repo.get(fspath(erepo_dir), fspath(external_data), "ext_data_imported")
    assert (tmp_dir / "ext_data_imported").is_file()
    assert (tmp_dir / "ext_data_imported").read_text() == "ext_data"


def test_non_cached_output(tmp_dir, erepo_dir, monkeypatch):
    src = "non_cached_file"
    dst = src + "_imported"

    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.dvc.run(
            outs_no_cache=[src], cmd="echo hello > non_cached_file"
        )
        erepo_dir.scm.add([src, src + ".dvc"])
        erepo_dir.scm.commit("add non-cached output")

    Repo.get(fspath(erepo_dir), src, dst)

    assert (tmp_dir / dst).is_file()
    # NOTE: using strip() to account for `echo` differences on win and *nix
    assert (tmp_dir / dst).read_text().strip() == "hello"


# https://github.com/iterative/dvc/pull/2837#discussion_r352123053
def test_absolute_file_outside_repo(tmp_dir, erepo_dir):
    with pytest.raises(PathMissingError):
        Repo.get(fspath(erepo_dir), "/root/")


def test_unknown_path(tmp_dir, erepo_dir):
    with pytest.raises(PathMissingError):
        Repo.get(fspath(erepo_dir), "a_non_existing_file")


@pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
def test_get_to_dir(tmp_dir, erepo_dir, monkeypatch, dname):
    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.dvc_gen("file", "contents", commit="create file")

    makedirs(dname, exist_ok=True)

    Repo.get(fspath(erepo_dir), "file", dname)

    assert (tmp_dir / dname).is_dir()
    assert (tmp_dir / dname / "file").read_text() == "contents"


def test_get_from_non_dvc_master(
    tmp_dir, erepo_dir, tmp_path, monkeypatch, caplog
):
    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.scm.checkout("new_branch", create_new=True)
        erepo_dir.scm_gen(
            {"some_file": "some_contents"}, commit="create some file"
        )
        erepo_dir.scm.checkout("master")

        erepo_dir.dvc.scm.repo.index.remove([".dvc"], r=True)
        erepo_dir.dvc.scm.commit("remove .dvc")

    # sanity check
    with pytest.raises(UrlNotDvcRepoError):
        Repo.get(fspath(erepo_dir), "some_file")

    caplog.clear()
    dst = "file_imported"
    with caplog.at_level(logging.INFO, logger="dvc"):
        Repo.get(fspath(erepo_dir), "some_file", out=dst, rev="new_branch")

    assert caplog.text == ""
    assert (tmp_dir / dst).read_text() == "some_contents"
