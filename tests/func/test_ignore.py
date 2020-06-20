import os
import shutil

import pytest

from dvc.exceptions import DvcIgnoreInCollectedDirError
from dvc.ignore import (
    DvcIgnore,
    DvcIgnoreDirs,
    DvcIgnorePatterns,
    DvcIgnoreRepo,
)
from dvc.repo import Repo
from dvc.scm.tree import WorkingTree
from dvc.utils import relpath
from dvc.utils.fs import get_mtime_and_size
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

    assert len(dvc.tree.dvcignore.ignores) == 3
    assert DvcIgnoreDirs([".git", ".hg", ".dvc"]) in dvc.tree.dvcignore.ignores
    assert (
        DvcIgnorePatterns(
            os.fspath(top_ignore_file), WorkingTree(dvc.root_dir)
        )
        in dvc.tree.dvcignore.ignores
    )
    assert any(
        i for i in dvc.tree.dvcignore.ignores if isinstance(i, DvcIgnoreRepo)
    )


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

    result = {os.fspath(os.path.normpath(f)) for f in dvc.tree.walk_files(".")}
    assert result == {".dvcignore", "foo"}


def test_ignore_external(tmp_dir, scm, dvc, tmp_path_factory):
    tmp_dir.gen(".dvcignore", "*.backup\ntmp")
    ext_dir = TmpDir(os.fspath(tmp_path_factory.mktemp("external_dir")))
    ext_dir.gen({"y.backup": "y", "tmp": "ext tmp"})

    result = {relpath(f, ext_dir) for f in dvc.tree.walk_files(ext_dir)}
    assert result == {"y.backup", "tmp"}


def test_ignore_subrepo(tmp_dir, scm, dvc):
    tmp_dir.gen({".dvcignore": "foo", "subdir": {"foo": "foo"}})
    scm.add([".dvcignore"])
    scm.commit("init parent dvcignore")

    subrepo_dir = tmp_dir / "subdir"
    assert not dvc.tree.exists(subrepo_dir / "foo")

    with subrepo_dir.chdir():
        subrepo = Repo.init(subdir=True)
        scm.add(str(subrepo_dir / "foo"))
        scm.commit("subrepo init")

    for _ in subrepo.brancher(all_commits=True):
        assert subrepo.tree.exists(subrepo_dir / "foo")


def test_ignore_blank_line(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"ignored": "text", "other": "text2"}})
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "foo\n\ndir/ignored")

    assert _files_set("dir", dvc.tree) == {"dir/other"}
