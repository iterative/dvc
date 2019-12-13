# encoding: utf-8
from __future__ import unicode_literals
import os
import shutil
import pytest

from dvc.exceptions import DvcIgnoreInCollectedDirError
from dvc.ignore import DvcIgnore, DvcIgnoreDirs, DvcIgnorePatterns
from dvc.scm.tree import WorkingTree
from dvc.utils import walk_files
from dvc.utils.compat import fspath
from dvc.utils.fs import get_mtime_and_size

from tests.utils import to_posixpath


def test_ignore(tmp_dir, dvc, monkeypatch):
    tmp_dir.gen({"dir": {"ignored": "text", "other": "text2"}})
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "dir/ignored")

    assert _files_set("dir", dvc.dvcignore) == {"dir/other"}

    monkeypatch.chdir("dir")
    assert _files_set(".", dvc.dvcignore) == {"./other"}


def test_ignore_unicode(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"other": "text"}})
    # Path() doesn't handle unicode paths in Windows/Python 2
    # I don't know to debug it further, waiting till Python 2 EOL
    with open("dir/тест", "wb") as fd:
        fd.write("проверка".encode("utf-8"))

    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "dir/тест")

    assert _files_set("dir", dvc.dvcignore) == {"dir/other"}


def test_rename_ignored_file(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"ignored": "...", "other": "text"}})

    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "ignored*")
    mtime, size = get_mtime_and_size("dir", dvc.dvcignore)

    shutil.move("dir/ignored", "dir/ignored_new")
    new_mtime, new_size = get_mtime_and_size("dir", dvc.dvcignore)

    assert new_mtime == mtime and new_size == size


def test_rename_file(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    mtime, size = get_mtime_and_size("dir", dvc.dvcignore)

    shutil.move("dir/foo", "dir/foo_new")
    new_mtime, new_size = get_mtime_and_size("dir", dvc.dvcignore)

    assert new_mtime != mtime and new_size == size


def test_remove_ignored_file(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"ignored": "...", "other": "text"}})
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "dir/ignored")

    mtime, size = get_mtime_and_size("dir", dvc.dvcignore)

    os.remove("dir/ignored")
    new_mtime, new_size = get_mtime_and_size("dir", dvc.dvcignore)

    assert new_mtime == mtime and new_size == size


def test_remove_file(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    mtime, size = get_mtime_and_size("dir", dvc.dvcignore)

    os.remove("dir/foo")
    new_mtime, new_size = get_mtime_and_size("dir", dvc.dvcignore)

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

    assert dvc.dvcignore.ignores == {
        DvcIgnoreDirs([".git", ".hg", ".dvc"]),
        DvcIgnorePatterns(fspath(top_ignore_file), WorkingTree(dvc.root_dir)),
    }


def test_ignore_on_branch(tmp_dir, scm, dvc):
    tmp_dir.scm_gen({"foo": "foo", "bar": "bar"}, commit="add files")

    scm.checkout("branch", create_new=True)
    tmp_dir.scm_gen(DvcIgnore.DVCIGNORE_FILE, "foo", commit="add ignore")

    scm.checkout("master")
    assert _files_set(".", dvc.dvcignore) == {"./foo", "./bar"}

    dvc.tree = scm.get_tree("branch")
    assert _files_set(".", dvc.dvcignore) == {"./bar"}


def _files_set(root, dvcignore):
    return {to_posixpath(f) for f in walk_files(root, dvcignore)}
