import os
import shutil

import pytest

from dvc.exceptions import DvcIgnoreInCollectedDirError
from dvc.ignore import (
    DvcIgnore,
    DvcIgnoreDirs,
    DvcIgnorePatterns,
    DvcIgnorePatternsTrie,
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
    ignore_pattern_trie = None
    for ignore in dvc.tree.dvcignore.ignores:
        if isinstance(ignore, DvcIgnorePatternsTrie):
            ignore_pattern_trie = ignore

    assert ignore_pattern_trie is not None
    assert (
        DvcIgnorePatterns.from_files(
            os.fspath(top_ignore_file), WorkingTree(dvc.root_dir)
        )
        == ignore_pattern_trie[os.fspath(ignore_file)]
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


def test_multi_ignore_file(tmp_dir, dvc, monkeypatch):
    tmp_dir.gen({"dir": {"subdir": {"should_ignore": "1", "not_ignore": "1"}}})
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "dir/subdir/*_ignore")
    tmp_dir.gen({"dir": {DvcIgnore.DVCIGNORE_FILE: "!subdir/not_ignore"}})

    assert _files_set("dir", dvc.tree) == {
        "dir/subdir/not_ignore",
        "dir/{}".format(DvcIgnore.DVCIGNORE_FILE),
    }


@pytest.mark.parametrize(
    "patterns, dirname, changed",
    [
        # A line starting with # serves as a comment.
        ("#comment", "dir", "#comment"),
        # Put a backslash ("\") in front of the first hash for patterns that
        # begin with a hash.
        ("\\#hash", "dir", "/dir/**/#hash"),
        ("\\#hash", "#dir", "/#dir/**/#hash"),
        # Trailing spaces are ignored unless they are quoted with
        # backslash ("\").
        (" space", "dir", "/dir/**/space"),
        ("\\ space", "dir", "/dir/**/ space"),
        # An optional prefix "!" which negates the pattern;
        ("!include", "dir", "!/dir/**/include"),
        # Put a backslash ("\") in front of the first "!" for patterns that
        # begin with a literal "!", for example, "\!important!.txt".
        ("\\!important!.txt", "dir", "/dir/**/!important!.txt"),
        # If there is a separator at the beginning or middle (or both) of the
        # pattern, then the pattern is relative to the directory level of the
        # particular .gitignore file itself.
        ("/separator.txt", "dir", "/dir/separator.txt"),
        ("subdir/separator.txt", "dir", "/dir/subdir/separator.txt"),
        # Otherwise the pattern may also match at any level below
        # the .gitignore level.
        ("no_sep", "dir", "/dir/**/no_sep"),
        # If there is a separator at the end of the pattern then the pattern
        # will only match directories, otherwise the pattern can match both
        # files and directories.
        ("doc/fortz/", "dir", "/dir/doc/fortz/"),
        ("fortz/", "dir", "/dir/**/fortz/"),
        # An asterisk "*" matches anything except a slash.
        ("*aste*risk*", "dir", "/dir/**/*aste*risk*"),
        # The character "?" matches any one character except "/".
        ("?fi?le?", "dir", "/dir/**/?fi?le?"),
        # The range notation, e.g. [a-zA-Z], can be used to match one of the
        # characters in a range. See fnmatch(3) and the FNM_PATHNAME flag
        # for a more detailed description.
        ("[a-zA-Z]file[a-zA-Z]", "dir", "/dir/**/[a-zA-Z]file[a-zA-Z]"),
        # Two consecutive asterisks ("**") in patterns matched against full
        # pathname may have special meaning:
        # A leading "**" followed by a slash means match in all directories.
        # For example, "**/foo" matches file or directory "foo" anywhere,
        # the same as pattern "foo".
        ("**/foo", "dir", "/dir/**/foo"),
        # "**/foo/bar" matches file or directory "bar" anywhere that is
        # directly under directory "foo".
        ("**/foo/bar", "dir", "/dir/**/foo/bar"),
        # A trailing "/**" matches everything inside.
        # For example, "abc/**" matches all files inside directory "abc",
        # relative to the location of the .gitignore file, with infinite depth.
        ("abc/**", "dir", "/dir/abc/**"),
        # A slash followed by two consecutive asterisks then a slash matches
        # zero or more directories. For example, "a/**/b"
        # matches "a/b", "a/x/b", "a/x/y/b" and so on.
        ("a/**/b", "dir", "/dir/a/**/b"),
        # Other consecutive asterisks are considered regular asterisks and
        # will match according to the previous rules.
        ("/***.txt", "dir", "/dir/***.txt"),
        ("data/***", "dir", "/dir/data/***"),
        ("***/file.txt", "dir", "/dir/***/file.txt"),
        ("***file", "dir", "/dir/**/***file"),
        ("a/***/b", "dir", "/dir/a/***/b"),
    ],
)
def test_dvcignore_pattern_change_dir(
    tmp_dir, dvc, patterns, dirname, changed
):
    tmp_dir.gen(
        {
            "parent": {
                dirname: {DvcIgnore.DVCIGNORE_FILE: patterns},
                "subdir": {},
            }
        }
    )
    ignore_pattern_trie = None
    for ignore in dvc.tree.dvcignore.ignores:
        if isinstance(ignore, DvcIgnorePatternsTrie):
            ignore_pattern_trie = ignore
            break

    assert ignore_pattern_trie is not None
    ignore_pattern = ignore_pattern_trie[
        os.fspath(tmp_dir / "parent" / dirname)
    ]
    ignore_pattern_changed = ignore_pattern.change_dirname(
        os.fspath(tmp_dir / "parent")
    )
    assert (
        DvcIgnorePatterns([changed], os.fspath(tmp_dir / "parent"))
        == ignore_pattern_changed
    )


def test_dvcignore_pattern_merge(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "top": {
                "first": {
                    DvcIgnore.DVCIGNORE_FILE: "a\nb\nc",
                    "middle": {
                        "second": {
                            DvcIgnore.DVCIGNORE_FILE: "d\ne\nf",
                            "bottom": {},
                        }
                    },
                },
            },
            "other": {DvcIgnore.DVCIGNORE_FILE: "1\n2\n3"},
        }
    )
    ignore_pattern_trie = None
    for ignore in dvc.tree.dvcignore.ignores:
        if isinstance(ignore, DvcIgnorePatternsTrie):
            ignore_pattern_trie = ignore
            break

    assert ignore_pattern_trie is not None
    ignore_pattern_top = ignore_pattern_trie[os.fspath(tmp_dir / "top")]
    ignore_pattern_other = ignore_pattern_trie[os.fspath(tmp_dir / "other")]
    ignore_pattern_first = ignore_pattern_trie[
        os.fspath(tmp_dir / "top" / "first")
    ]
    ignore_pattern_middle = ignore_pattern_trie[
        os.fspath(tmp_dir / "top" / "first" / "middle")
    ]
    ignore_pattern_second = ignore_pattern_trie[
        os.fspath(tmp_dir / "top" / "first" / "middle" / "second")
    ]
    ignore_pattern_bottom = ignore_pattern_trie[
        os.fspath(tmp_dir / "top" / "first" / "middle" / "second" / "bottom")
    ]
    assert not ignore_pattern_top
    assert (
        DvcIgnorePatterns([], os.fspath(tmp_dir / "top")) == ignore_pattern_top
    )
    assert (
        DvcIgnorePatterns(["1", "2", "3"], os.fspath(tmp_dir / "other"))
        == ignore_pattern_other
    )
    assert (
        DvcIgnorePatterns(
            ["a", "b", "c"], os.fspath(tmp_dir / "top" / "first")
        )
        == ignore_pattern_first
    )
    assert (
        DvcIgnorePatterns(
            ["a", "b", "c"], os.fspath(tmp_dir / "top" / "first")
        )
        == ignore_pattern_middle
    )
    assert (
        DvcIgnorePatterns(
            [
                "a",
                "b",
                "c",
                "/middle/second/**/d",
                "/middle/second/**/e",
                "/middle/second/**/f",
            ],
            os.fspath(tmp_dir / "top" / "first"),
        )
        == ignore_pattern_second
    )
    assert (
        DvcIgnorePatterns(
            [
                "a",
                "b",
                "c",
                "/middle/second/**/d",
                "/middle/second/**/e",
                "/middle/second/**/f",
            ],
            os.fspath(tmp_dir / "top" / "first"),
        )
        == ignore_pattern_bottom
    )
