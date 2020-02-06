import logging
import os

import pytest

from dvc.cache import Cache
from dvc.config import Config
from dvc.main import main
from dvc.exceptions import PathMissingError
from dvc.repo.get import GetDVCFileError
from dvc.repo import Repo
from dvc.system import System
from dvc.utils.fs import makedirs
from dvc.compat import fspath
from tests.utils import trees_equal


def test_get_repo_file(tmp_dir, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("file", "contents", commit="create file")

    Repo.get(fspath(erepo_dir), "file", "file_imported")

    assert os.path.isfile("file_imported")
    assert (tmp_dir / "file_imported").read_text() == "contents"


def test_get_repo_dir(tmp_dir, erepo_dir):
    with erepo_dir.chdir():
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


def test_cache_type_is_properly_overridden(tmp_dir, erepo_dir):
    with erepo_dir.chdir():
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


def test_get_repo_rev(tmp_dir, erepo_dir):
    with erepo_dir.chdir(), erepo_dir.branch("branch", new=True):
        erepo_dir.dvc_gen("file", "contents", commit="create file on branch")

    Repo.get(fspath(erepo_dir), "file", "file_imported", rev="branch")
    assert (tmp_dir / "file_imported").read_text() == "contents"


def test_get_from_non_dvc_repo(tmp_dir, git_dir):
    git_dir.scm_gen({"some_file": "contents"}, commit="create file")

    Repo.get(fspath(git_dir), "some_file", "file_imported")
    assert (tmp_dir / "file_imported").read_text() == "contents"


def test_get_a_dvc_file(tmp_dir, erepo_dir):
    with pytest.raises(GetDVCFileError):
        Repo.get(fspath(erepo_dir), "some_file.dvc")


# https://github.com/iterative/dvc/pull/2837#discussion_r352123053
def test_get_full_dvc_path(tmp_dir, erepo_dir, tmp_path_factory):
    path = tmp_path_factory.mktemp("ext")
    external_data = path / "ext_data"
    external_data.write_text("ext_data")

    with erepo_dir.chdir():
        erepo_dir.dvc_add(fspath(external_data), commit="add external data")

    Repo.get(fspath(erepo_dir), fspath(external_data), "ext_data_imported")
    assert (tmp_dir / "ext_data_imported").read_text() == "ext_data"


def test_non_cached_output(tmp_dir, erepo_dir):
    src = "non_cached_file"
    dst = src + "_imported"

    with erepo_dir.chdir():
        erepo_dir.dvc.run(
            outs_no_cache=[src], cmd="echo hello > non_cached_file"
        )
        erepo_dir.scm_add([src, src + ".dvc"], commit="add non-cached output")

    Repo.get(fspath(erepo_dir), src, dst)

    assert (tmp_dir / dst).is_file()
    # NOTE: using strip() to account for `echo` differences on win and *nix
    assert (tmp_dir / dst).read_text().strip() == "hello"


# https://github.com/iterative/dvc/pull/2837#discussion_r352123053
def test_absolute_file_outside_repo(tmp_dir, erepo_dir):
    with pytest.raises(PathMissingError):
        Repo.get(fspath(erepo_dir), "/root/")


def test_absolute_file_outside_git_repo(tmp_dir, git_dir):
    with pytest.raises(PathMissingError):
        Repo.get(fspath(git_dir), "/root/")


def test_unknown_path(tmp_dir, erepo_dir):
    with pytest.raises(PathMissingError):
        Repo.get(fspath(erepo_dir), "a_non_existing_file")


@pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
def test_get_to_dir(tmp_dir, erepo_dir, dname):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("file", "contents", commit="create file")

    makedirs(dname, exist_ok=True)

    Repo.get(fspath(erepo_dir), "file", dname)

    assert (tmp_dir / dname).is_dir()
    assert (tmp_dir / dname / "file").read_text() == "contents"


def test_get_from_non_dvc_master(tmp_dir, git_dir, caplog):
    with git_dir.chdir(), git_dir.branch("branch", new=True):
        git_dir.init(dvc=True)
        git_dir.dvc_gen("some_file", "some text", commit="create some file")

    caplog.clear()

    with caplog.at_level(logging.INFO, logger="dvc"):
        Repo.get(fspath(git_dir), "some_file", out="some_dst", rev="branch")

    assert caplog.text == ""
    assert (tmp_dir / "some_dst").read_text() == "some text"


def test_get_url_positive(tmp_dir, erepo_dir, caplog):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo")

    caplog.clear()
    with caplog.at_level(logging.ERROR, logger="dvc"):
        assert main(["get", fspath(erepo_dir), "foo", "--show-url"]) == 0
        assert caplog.text == ""


def test_get_url_not_existing(tmp_dir, erepo_dir, caplog):
    with caplog.at_level(logging.ERROR, logger="dvc"):
        assert (
            main(["get", fspath(erepo_dir), "not-existing-file", "--show-url"])
            == 1
        )
        assert "failed to show URL" in caplog.text


def test_get_url_git_only_repo(tmp_dir, scm, caplog):
    tmp_dir.scm_gen({"foo": "foo"}, commit="initial")

    with caplog.at_level(logging.ERROR):
        assert main(["get", fspath(tmp_dir), "foo", "--show-url"]) == 1
        assert "failed to show URL" in caplog.text
