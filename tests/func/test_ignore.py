# encoding: utf-8
import os
import shutil
import pytest

from dvc.exceptions import DvcIgnoreInCollectedDirError
from dvc.ignore import DvcIgnore, DvcIgnoreDirs, DvcIgnorePatterns
from dvc.scm.tree import WorkingTree
from dvc.utils import relpath
from dvc.compat import fspath_py35, fspath
from dvc.utils.fs import get_mtime_and_size
from dvc.remote import RemoteLOCAL

from tests.dir_helpers import TmpDir
from tests.utils import to_posixpath


def test_ignore(tmp_dir, dvc, monkeypatch):
    tmp_dir.gen({"dir": {"ignored": "text", "other": "text2"}})
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "dir/ignored")

    assert _files_set("dir", dvc.tree) == {"dir/other"}

    monkeypatch.chdir("dir")
    assert _files_set(".", dvc.tree) == {"./other"}


def test_ignore_unicode(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"other": "text", "тест": "проверка"}})
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "dir/тест")

    assert _files_set("dir", dvc.tree) == {"dir/other"}


def test_rename_ignored_file(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"ignored": "...", "other": "text"}})

    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "ignored*")
    mtime, size = get_mtime_and_size("dir", dvc.tree)

    shutil.move("dir/ignored", "dir/ignored_new")
    new_mtime, new_size = get_mtime_and_size("dir", dvc.tree)

    assert new_mtime == mtime and new_size == size


def test_rename_file(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    mtime, size = get_mtime_and_size("dir", dvc.tree)

    shutil.move("dir/foo", "dir/foo_new")
    new_mtime, new_size = get_mtime_and_size("dir", dvc.tree)

    assert new_mtime != mtime and new_size == size


def test_remove_ignored_file(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"ignored": "...", "other": "text"}})
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "dir/ignored")

    mtime, size = get_mtime_and_size("dir", dvc.tree)

    os.remove("dir/ignored")
    new_mtime, new_size = get_mtime_and_size("dir", dvc.tree)

    assert new_mtime == mtime and new_size == size


def test_remove_file(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    mtime, size = get_mtime_and_size("dir", dvc.tree)

    os.remove("dir/foo")
    new_mtime, new_size = get_mtime_and_size("dir", dvc.tree)

    assert new_mtime != mtime and new_size != size


def test_dvcignore_in_out_dir(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", DvcIgnore.DVCIGNORE_FILE: ""}})

    with pytest.raises(DvcIgnoreInCollectedDirError):
        dvc.add("dir")


@pytest.mark.parametrize("dname", ["dir", "dir/subdir"])
def test_ignore_collecting_dvcignores(tmp_dir, dvc, dname):
    tmp_dir.gen({"dir": {"subdir": {}}})

    top_ignore_file = (tmp_dir / dname).with_name(DvcIgnore.DVCIGNORE_FILE)
    top_ignore_file.write_text(os.path.basename(dname))

    ignore_file = tmp_dir / dname / DvcIgnore.DVCIGNORE_FILE
    ignore_file.write_text("foo")

    assert dvc.tree.dvcignore.ignores == {
        DvcIgnoreDirs([".git", ".hg", ".dvc"]),
        DvcIgnorePatterns(fspath(top_ignore_file), WorkingTree(dvc.root_dir)),
    }


def test_ignore_on_branch(tmp_dir, scm, dvc):
    tmp_dir.scm_gen({"foo": "foo", "bar": "bar"}, commit="add files")

    with tmp_dir.branch("branch", new=True):
        tmp_dir.scm_gen(DvcIgnore.DVCIGNORE_FILE, "foo", commit="add ignore")

    assert _files_set(".", dvc.tree) == {"./foo", "./bar"}

    dvc.tree = scm.get_tree("branch")
    assert _files_set(".", dvc.tree) == {
        to_posixpath(os.path.join(dvc.root_dir, DvcIgnore.DVCIGNORE_FILE)),
        to_posixpath(os.path.join(dvc.root_dir, "bar")),
    }


def _files_set(root, tree):
    return {to_posixpath(f) for f in tree.walk_files(root)}


def test_match_nested(tmp_dir, dvc):
    tmp_dir.gen(
        {
            ".dvcignore": "*.backup\ntmp",
            "foo": "foo",
            "tmp": "...",
            "dir": {"x.backup": "x backup", "tmp": "content"},
        }
    )

    remote = RemoteLOCAL(dvc, {})
    result = {fspath(f) for f in remote.walk_files(".")}
    assert result == {".dvcignore", "foo"}


def test_ignore_external(tmp_dir, scm, dvc, tmp_path_factory):
    tmp_dir.gen(".dvcignore", "*.backup\ntmp")
    ext_dir = TmpDir(fspath_py35(tmp_path_factory.mktemp("external_dir")))
    ext_dir.gen({"y.backup": "y", "tmp": "ext tmp"})

    remote = RemoteLOCAL(dvc, {})
    result = {relpath(f, ext_dir) for f in remote.walk_files(ext_dir)}
    assert result == {"y.backup", "tmp"}
