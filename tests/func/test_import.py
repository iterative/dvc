import filecmp
import os
import shutil
from dvc.compat import fspath

import pytest
from mock import patch

from dvc.cache import Cache
from dvc.config import Config
from dvc.exceptions import DownloadError, PathMissingError
from dvc.config import NoRemoteError
from dvc.stage import Stage
from dvc.system import System
from dvc.utils.fs import makedirs
import dvc.data_cloud as cloud
from tests.utils import trees_equal


def test_import(tmp_dir, scm, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo")

    stage = dvc.imp(fspath(erepo_dir), "foo", "foo_imported")

    assert os.path.isfile("foo_imported")
    assert (tmp_dir / "foo_imported").read_text() == "foo content"
    assert scm.repo.git.check_ignore("foo_imported")
    assert stage.deps[0].def_repo == {
        "url": fspath(erepo_dir),
        "rev_lock": erepo_dir.scm.get_rev(),
    }


@pytest.mark.parametrize("src_is_dvc", [True, False])
def test_import_git_file(tmp_dir, scm, dvc, git_dir, src_is_dvc):
    if src_is_dvc:
        git_dir.init(dvc=True)

    git_dir.scm_gen("src", "hello", commit="add a git file")

    stage = tmp_dir.dvc.imp(fspath(git_dir), "src", "dst")

    assert (tmp_dir / "dst").read_text() == "hello"
    assert tmp_dir.scm.repo.git.check_ignore(fspath(tmp_dir / "dst"))
    assert stage.deps[0].def_repo == {
        "url": fspath(git_dir),
        "rev_lock": git_dir.scm.get_rev(),
    }


def test_import_cached_file(erepo_dir, tmp_dir, dvc, scm, monkeypatch):
    src = "some_file"
    dst = "some_file_imported"

    with erepo_dir.chdir():
        erepo_dir.dvc_gen({src: "hello"}, commit="add a regular file")

    tmp_dir.dvc_gen({dst: "hello"})
    (tmp_dir / dst).unlink()

    remote_exception = NoRemoteError("dvc import")
    with patch.object(cloud.DataCloud, "pull", side_effect=remote_exception):
        tmp_dir.dvc.imp(fspath(erepo_dir), src, dst)

    assert (tmp_dir / dst).is_file()
    assert filecmp.cmp(
        fspath(erepo_dir / src), fspath(tmp_dir / dst), shallow=False
    )


@pytest.mark.parametrize("src_is_dvc", [True, False])
def test_import_git_dir(tmp_dir, scm, dvc, git_dir, src_is_dvc):
    if src_is_dvc:
        git_dir.init(dvc=True)

    git_dir.scm_gen({"src": {"file.txt": "hello"}}, commit="add a dir")

    stage = dvc.imp(fspath(git_dir), "src", "dst")

    assert (tmp_dir / "dst").is_dir()
    trees_equal(fspath(git_dir / "src"), fspath(tmp_dir / "dst"))
    assert tmp_dir.scm.repo.git.check_ignore(fspath(tmp_dir / "dst"))
    assert stage.deps[0].def_repo == {
        "url": fspath(git_dir),
        "rev_lock": git_dir.scm.get_rev(),
    }


def test_import_dir(tmp_dir, scm, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen({"dir": {"foo": "foo content"}}, commit="create dir")

    stage = dvc.imp(fspath(erepo_dir), "dir", "dir_imported")

    assert os.path.isdir("dir_imported")
    trees_equal(fspath(erepo_dir / "dir"), "dir_imported")
    assert scm.repo.git.check_ignore("dir_imported")
    assert stage.deps[0].def_repo == {
        "url": fspath(erepo_dir),
        "rev_lock": erepo_dir.scm.get_rev(),
    }


def test_import_non_cached(erepo_dir, tmp_dir, dvc, scm):
    src = "non_cached_output"
    dst = src + "_imported"

    erepo_dir.dvc.run(
        cmd="echo hello > {}".format(src),
        outs_no_cache=[src],
        cwd=fspath(erepo_dir),
    )

    erepo_dir.scm_add([fspath(erepo_dir / src)], commit="add a non-cached out")

    stage = tmp_dir.dvc.imp(fspath(erepo_dir), src, dst)

    assert (tmp_dir / dst).is_file()
    assert filecmp.cmp(
        fspath(erepo_dir / src), fspath(tmp_dir / dst), shallow=False
    )
    assert tmp_dir.scm.repo.git.check_ignore(dst)
    assert stage.deps[0].def_repo == {
        "url": fspath(erepo_dir),
        "rev_lock": erepo_dir.scm.get_rev(),
    }


def test_import_rev(tmp_dir, scm, dvc, erepo_dir):
    rev = None
    with erepo_dir.chdir(), erepo_dir.branch("branch", new=True):
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo on branch")
        rev = erepo_dir.scm.get_rev()

    stage = dvc.imp(fspath(erepo_dir), "foo", "foo_imported", rev="branch")

    assert (tmp_dir / "foo_imported").read_text() == "foo content"
    assert scm.repo.git.check_ignore("foo_imported")
    assert stage.deps[0].def_repo == {
        "url": fspath(erepo_dir),
        "rev": "branch",
        "rev_lock": rev,
    }


def test_pull_imported_stage(tmp_dir, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo")
    dvc.imp(fspath(erepo_dir), "foo", "foo_imported")

    dst_stage = Stage.load(dvc, "foo_imported.dvc")
    dst_cache = dst_stage.outs[0].cache_path

    os.remove("foo_imported")
    os.remove(dst_cache)
    dvc.pull(["foo_imported.dvc"])

    assert os.path.isfile("foo_imported")
    assert os.path.isfile(dst_cache)


def test_cache_type_is_properly_overridden(tmp_dir, scm, dvc, erepo_dir):
    with erepo_dir.chdir():
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


def test_pull_imported_directory_stage(tmp_dir, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen({"dir": {"foo": "foo content"}}, commit="create dir")

    dvc.imp(fspath(erepo_dir), "dir", "dir_imported")

    shutil.rmtree("dir_imported")
    shutil.rmtree(dvc.cache.local.cache_dir)

    dvc.pull(["dir_imported.dvc"])

    assert os.path.isdir("dir_imported")
    trees_equal(fspath(erepo_dir / "dir"), "dir_imported")


def test_download_error_pulling_imported_stage(tmp_dir, dvc, erepo_dir):
    with erepo_dir.chdir():
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
def test_import_to_dir(dname, tmp_dir, dvc, erepo_dir):
    makedirs(dname, exist_ok=True)

    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo")

    stage = dvc.imp(fspath(erepo_dir), "foo", dname)

    dst = os.path.join(dname, "foo")

    assert stage.outs[0].fspath == os.path.abspath(dst)
    assert os.path.isdir(dname)
    assert (tmp_dir / dst).read_text() == "foo content"


def test_pull_non_workspace(tmp_dir, scm, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "master content", commit="create foo")

        with erepo_dir.branch("branch", new=True):
            erepo_dir.dvc_gen("foo", "branch content", commit="modify foo")

    stage = dvc.imp(fspath(erepo_dir), "foo", "foo_imported", rev="branch")
    tmp_dir.scm_add([stage.relpath], commit="imported branch")
    scm.tag("ref-to-branch")

    # Overwrite via import
    dvc.imp(fspath(erepo_dir), "foo", "foo_imported", rev="master")

    os.remove(stage.outs[0].cache_path)
    dvc.fetch(all_tags=True)
    assert os.path.exists(stage.outs[0].cache_path)


def test_import_non_existing(erepo_dir, tmp_dir, dvc):
    with pytest.raises(PathMissingError):
        tmp_dir.dvc.imp(fspath(erepo_dir), "invalid_output")

    # https://github.com/iterative/dvc/pull/2837#discussion_r352123053
    with pytest.raises(PathMissingError):
        tmp_dir.dvc.imp(fspath(erepo_dir), "/root/", "root")


def test_pull_no_rev_lock(erepo_dir, tmp_dir, dvc):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "contents", commit="create foo")

    stage = dvc.imp(fspath(erepo_dir), "foo", "foo_imported")
    assert "rev" not in stage.deps[0].def_repo
    stage.deps[0].def_repo.pop("rev_lock")
    stage.dump()

    os.remove(stage.outs[0].cache_path)
    (tmp_dir / "foo_imported").unlink()

    dvc.pull([stage.path])

    assert (tmp_dir / "foo_imported").is_file()
    assert (tmp_dir / "foo_imported").read_text() == "contents"
