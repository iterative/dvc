from __future__ import unicode_literals

import filecmp
import os
import shutil

import pytest
from mock import patch

from dvc.cache import Cache
from dvc.config import Config
from dvc.exceptions import DownloadError
from dvc.exceptions import PathMissingError
from dvc.exceptions import NoOutputInExternalRepoError
from dvc.stage import Stage
from dvc.system import System
from dvc.utils import makedirs
from dvc.utils.compat import fspath
from tests.utils import trees_equal


def test_import(tmp_dir, scm, dvc, erepo_dir, monkeypatch):
    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo")

    dvc.imp(fspath(erepo_dir), "foo", "foo_imported")

    assert os.path.isfile("foo_imported")
    assert (tmp_dir / "foo_imported").read_text() == "foo content"
    assert scm.repo.git.check_ignore("foo_imported")


def test_import_git_file(erepo_dir, tmp_dir, dvc, scm):
    src = "some_file"
    dst = "some_file_imported"

    erepo_dir.scm_gen({src: "hello"}, commit="add a regular file")

    tmp_dir.dvc.imp(fspath(erepo_dir), src, dst)

    assert (tmp_dir / dst).is_file()
    assert filecmp.cmp(
        fspath(erepo_dir / src), fspath(tmp_dir / dst), shallow=False
    )
    assert tmp_dir.scm.repo.git.check_ignore(fspath(tmp_dir / dst))


def test_import_git_dir(erepo_dir, tmp_dir, dvc, scm):
    src = "some_directory"
    dst = "some_directory_imported"

    erepo_dir.scm_gen({src: {"file.txt": "hello"}}, commit="add a dir")

    tmp_dir.dvc.imp(fspath(erepo_dir), src, dst)

    assert (tmp_dir / dst).is_dir()
    trees_equal(fspath(erepo_dir / src), fspath(tmp_dir / dst))
    assert tmp_dir.scm.repo.git.check_ignore(fspath(tmp_dir / dst))


def test_import_dir(tmp_dir, scm, dvc, erepo_dir, monkeypatch):
    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.dvc_gen({"dir": {"foo": "foo content"}}, commit="create dir")

    dvc.imp(fspath(erepo_dir), "dir", "dir_imported")

    assert os.path.isdir("dir_imported")
    trees_equal(fspath(erepo_dir / "dir"), "dir_imported")
    assert scm.repo.git.check_ignore("dir_imported")


def test_import_non_cached(erepo_dir, tmp_dir, dvc, scm):
    src = "non_cached_output"
    dst = src + "_imported"

    erepo_dir.dvc.run(
        cmd="echo hello > {}".format(src),
        outs_no_cache=[src],
        cwd=fspath(erepo_dir),
    )

    erepo_dir.scm.add([fspath(erepo_dir / src)])
    erepo_dir.scm.commit("add a non-cached output")

    tmp_dir.dvc.imp(fspath(erepo_dir), src, dst)

    assert (tmp_dir / dst).is_file()
    assert filecmp.cmp(
        fspath(erepo_dir / src), fspath(tmp_dir / dst), shallow=False
    )
    assert tmp_dir.scm.repo.git.check_ignore(dst)


def test_import_rev(tmp_dir, scm, dvc, erepo_dir, monkeypatch):
    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.scm.checkout("new_branch", create_new=True)
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo on branch")
        erepo_dir.scm.checkout("master")

    dvc.imp(fspath(erepo_dir), "foo", "foo_imported", rev="new_branch")

    assert (tmp_dir / "foo_imported").read_text() == "foo content"
    assert scm.repo.git.check_ignore("foo_imported")


def test_pull_imported_stage(tmp_dir, dvc, erepo_dir, monkeypatch):
    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo")
    dvc.imp(fspath(erepo_dir), "foo", "foo_imported")

    dst_stage = Stage.load(dvc, "foo_imported.dvc")
    dst_cache = dst_stage.outs[0].cache_path

    os.remove("foo_imported")
    os.remove(dst_cache)
    dvc.pull(["foo_imported.dvc"])

    assert os.path.isfile("foo_imported")
    assert os.path.isfile(dst_cache)


def test_cache_type_is_properly_overridden(
    tmp_dir, scm, dvc, erepo_dir, monkeypatch
):
    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.dvc.config.set(
            Config.SECTION_CACHE, Config.SECTION_CACHE_TYPE, "symlink"
        )
        erepo_dir.dvc.cache = Cache(erepo_dir.dvc)
        erepo_dir.scm_add(
            [erepo_dir.dvc.config.config_file],
            "set source repo cache type to symlink",
        )
        erepo_dir.dvc_gen("foo", "foo content", "create foo")
    assert System.is_symlink(erepo_dir / "foo")

    dvc.imp(fspath(erepo_dir), "foo", "foo_imported")

    assert not System.is_symlink("foo_imported")
    assert (tmp_dir / "foo_imported").read_text() == "foo content"
    assert scm.repo.git.check_ignore("foo_imported")


def test_pull_imported_directory_stage(tmp_dir, dvc, erepo_dir, monkeypatch):
    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.dvc_gen({"dir": {"foo": "foo content"}}, commit="create dir")

    dvc.imp(fspath(erepo_dir), "dir", "dir_imported")

    shutil.rmtree("dir_imported")
    shutil.rmtree(dvc.cache.local.cache_dir)

    dvc.pull(["dir_imported.dvc"])

    assert os.path.isdir("dir_imported")
    trees_equal(fspath(erepo_dir / "dir"), "dir_imported")


def test_download_error_pulling_imported_stage(
    tmp_dir, dvc, erepo_dir, monkeypatch
):
    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo")
    dvc.imp(fspath(erepo_dir), "foo", "foo_imported")

    dst_stage = Stage.load(dvc, "foo_imported.dvc")
    dst_cache = dst_stage.outs[0].cache_path

    os.remove("foo_imported")
    os.remove(dst_cache)

    with patch(
        "dvc.remote.RemoteLOCAL._download", side_effect=Exception
    ), pytest.raises(DownloadError):
        dvc.pull(["foo_imported.dvc"])


@pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
def test_import_to_dir(dname, tmp_dir, dvc, erepo_dir, monkeypatch):
    makedirs(dname, exist_ok=True)

    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo")

    stage = dvc.imp(fspath(erepo_dir), "foo", dname)

    dst = os.path.join(dname, "foo")

    assert stage.outs[0].fspath == os.path.abspath(dst)
    assert os.path.isdir(dname)
    assert (tmp_dir / dst).read_text() == "foo content"


def test_pull_non_workspace(tmp_dir, scm, dvc, erepo_dir, monkeypatch):
    with monkeypatch.context() as m:
        m.chdir(fspath(erepo_dir))
        erepo_dir.dvc_gen("foo", "master content", commit="create foo")
        erepo_dir.scm.checkout("new_branch", create_new=True)
        erepo_dir.dvc_gen("foo", "branch content", commit="modify foo")

    stage = dvc.imp(fspath(erepo_dir), "foo", "foo_imported", rev="new_branch")
    tmp_dir.scm_add([stage.relpath], commit="imported branch")
    dvc.scm.tag("ref-to-branch")

    # Overwrite via import
    dvc.imp(fspath(erepo_dir), "foo", "foo_imported", rev="master")

    os.remove(stage.outs[0].cache_path)
    dvc.fetch(all_tags=True)
    assert os.path.exists(stage.outs[0].cache_path)


def test_import_non_existing(erepo_dir, tmp_dir, dvc):
    with pytest.raises(PathMissingError):
        tmp_dir.dvc.imp(fspath(erepo_dir), "invalid_output")
    # https://github.com/iterative/dvc/pull/2837#discussion_r352123053
    with pytest.raises(NoOutputInExternalRepoError):
        tmp_dir.dvc.imp(fspath(erepo_dir), "/root/", "root")
