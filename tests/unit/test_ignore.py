import os
from os.path import join

import pytest

from dvc.ignore import DvcIgnorePatterns


@pytest.mark.parametrize(
    "file_to_ignore_relpath, patterns,  expected_match",
    [
        # all rules from https://git-scm.com/docs/gitignore
        ("to_ignore", ["to_ignore"], True),
        ("dont_ignore.txt", ["dont_ignore"], False),
        # A blank line matches no files, so it can serve as a separator for
        # readability.
        ("to_ignore", ["", "to_ignore"], True),
        # A line starting with # serves as a comment.
        # Put a backslash ("\") in front of the first hash for patterns
        # that begin with a hash.
        ("#to_ignore", ["\\#to_ignore"], True),
        ("#to_ignore", ["#to_ignore"], False),
        # Trailing spaces are ignored unless they are quoted with
        # backslash ("\").
        (" to_ignore", [" to_ignore"], False),
        (" to_ignore", ["\\ to_ignore"], True),
        # An optional prefix "!" which negates the pattern; any matching file
        # excluded by a previous pattern will become included again.
        ("to_ignore.txt", ["to_ignore*"], True),
        ("to_ignore.txt", ["to_ignore*", "!to_ignore.txt"], False),
        ("to_ignore.txt", ["!to_ignore.txt", "to_ignore*"], True),
        # It is not possible to re-include a file if a parent directory of
        # that file is excluded.
        # Git doesn't list excluded directories for performance reasons,
        # so any patterns on contained files have no effect,
        # no matter where they are defined.
        # see (`tests/func/test_ignore.py::test_ignore_parent_path`)
        # Put a backslash ("\") in front of the first "!"
        # for patterns that begin with a literal "!",
        # for example, "\!important!.txt".
        ("!to_ignore.txt", ["\\!to_ignore.txt"], True),
        # The slash / is used as the directory separator.
        # Separators may occur at the beginning, middle or end of the
        # .gitignore search pattern.
        # If there is a separator at the beginning or middle (or both)
        # of the pattern, then the pattern is relative to the directory
        # level of the particular .gitignore file itself.
        # Otherwise the pattern may also match at any level below
        # the .gitignore level.
        ("file", ["/file"], True),
        (os.path.join("data", "file"), ["/file"], False),
        (os.path.join("data", "file"), ["data/file"], True),
        (os.path.join("other", "data", "file"), ["data/file"], False),
        (
            os.path.join(
                os.path.sep,
                "full",
                "path",
                "to",
                "ignore",
                "file",
                "to_ignore",
            ),
            ["to_ignore"],
            True,
        ),
        # If there is a separator at the end of the pattern then the pattern
        # will only match directories,
        # otherwise the pattern can match both files and directories.
        # For example, a pattern doc/frotz/ matches doc/frotz directory,
        # but not a/doc/frotz directory;
        # see (`tests/func/test_ignore.py::test_ignore_sub_directory`)
        # however frotz/ matches frotz and a/frotz that is a directory
        # (all paths are relative from the .gitignore file).
        # see (`tests/func/test_ignore.py::test_ignore_directory`)
        # An asterisk "*" matches anything except a slash.
        ("to_ignore.txt", ["/*.txt"], True),
        (os.path.join("path", "to_ignore.txt"), ["/*.txt"], False),
        (os.path.join("data", "file.txt"), ["data/*"], True),
        (os.path.join("data", "subdir", "file.txt"), ["data/*"], True),
        (os.path.join("data", "file.txt"), ["data/"], True),
        (os.path.join("data", "subdir", "file.txt"), ["data/"], True),
        (os.path.join("data", "subdir", "file.txt"), ["subdir/"], True),
        (os.path.join("data", "subdir", "file.txt"), ["/subdir/"], False),
        (os.path.join("data", "path"), ["path/"], False),
        (os.path.join(".git", "file.txt"), [".git/"], True),
        (os.path.join("data", ".dvc", "file.txt"), [".dvc/"], True),
        # wait for Git
        # (os.path.join("data", "sub", "file.txt"), ["data/*"], True),
        (
            os.path.join("rel", "path", "path2", "to_ignore"),
            ["rel/*/to_ignore"],
            False,
        ),
        ("file.txt", ["file.*"], True),
        # The character "?" matches any one character except "/".
        ("file.txt", ["fi?e.t?t"], True),
        ("fi/e.txt", ["fi?e.t?t"], False),
        # The range notation, e.g. [a-zA-Z], can be used
        # to match one of the characters in a range. See fnmatch(3) and
        # the FNM_PATHNAME flag for a more detailed description.
        ("file.txt", ["[a-zA-Z]ile.txt"], True),
        ("2ile.txt", ["[a-zA-Z]ile.txt"], False),
        # Two consecutive asterisks ("**") in patterns matched against
        # full pathname may have special meaning:
        # A leading "**" followed by a slash means match in all directories.
        # For example, "**/foo" matches file or directory "foo" anywhere, the
        # same as pattern "foo".
        # "**/foo/bar" matches file or directory "bar" anywhere that is
        # directly under directory "foo".
        (os.path.join("rel", "p", "p2", "to_ignore"), ["**/to_ignore"], True),
        (
            os.path.join("rel", "p", "p2", "to_ignore"),
            ["**/p2/to_ignore"],
            True,
        ),
        (
            os.path.join("rel", "path", "path2", "dont_ignore"),
            ["**/to_ignore"],
            False,
        ),
        # A trailing "/**" matches everything inside.
        # For example, "abc/**" matches all files inside directory "abc",
        # relative to the location of the .gitignore file, with infinite depth.
        (os.path.join("rel", "p", "p2", "to_ignore"), ["rel/**"], True),
        (os.path.join("rel", "p", "p2", "to_ignore"), ["p/**"], False),
        (
            os.path.join("rel", "path", "path2", "dont_ignore"),
            ["rel/**"],
            True,
        ),
        # A slash followed by two consecutive asterisks then a slash matches
        # zero or more directories.
        # For example, "a/**/b" matches "a/b", "a/x/b", "a/x/y/b" and so on.
        (os.path.join("rel", "p", "to_ignore"), ["rel/**/to_ignore"], True),
        (
            os.path.join("rel", "p", "p2", "to_ignore"),
            ["rel/**/to_ignore"],
            True,
        ),
        (
            os.path.join("rel", "path", "path2", "dont_ignore"),
            ["rel/**/to_ignore"],
            False,
        ),
        (
            os.path.join("rel", "path", "path2", "dont_ignore"),
            ["path/**/dont_ignore"],
            False,
        ),
        # Other consecutive asterisks are considered regular asterisks
        # and will match according to the previous rules.
        ("to_ignore.txt", ["/***.txt"], True),
        (os.path.join("path", "to_ignore.txt"), ["/****.txt"], False),
        (os.path.join("path", "to_ignore.txt"), ["****.txt"], True),
        (os.path.join("data", "file.txt"), ["data/***"], True),
        # bug from PathSpec
        # (os.path.join("data", "p", "file.txt"), ["data/***"], False),
        (os.path.join("data", "p", "file.txt"), ["***/file.txt"], False),
        (
            os.path.join("rel", "path", "path2", "to_ignore"),
            ["rel/***/to_ignore"],
            False,
        ),
    ],
)
def test_match_ignore_from_file(
    file_to_ignore_relpath, patterns, expected_match, mocker
):
    from dvc.fs import localfs

    root = r"\\" if os.name == "nt" else "/"
    dvcignore_path = os.path.join(
        root, "full", "path", "to", "ignore", "file", ".dvcignore"
    )
    dvcignore_dirname = os.path.dirname(dvcignore_path)

    mocker.patch.object(
        localfs, "open", mocker.mock_open(read_data="\n".join(patterns))
    )
    ignore_file = DvcIgnorePatterns.from_file(dvcignore_path, localfs, "mocked")

    assert (
        ignore_file.matches(dvcignore_dirname, file_to_ignore_relpath) == expected_match
    )


@pytest.mark.parametrize("sub_dir", ["", "dir"])
@pytest.mark.parametrize("omit_dir", [".git", ".hg", ".dvc"])
def test_should_ignore_dir(omit_dir, sub_dir):
    root = os.path.join(os.path.sep, "walk", "dir", "root")
    ignore = DvcIgnorePatterns([".git/", ".hg/", ".dvc/"], root, os.sep)

    dirs = [omit_dir, "dir1", "dir2"]
    files = [omit_dir, "file1", "file2"]

    if sub_dir:
        current = os.path.join(root, sub_dir)
    else:
        current = root

    new_dirs, new_files = ignore(current, dirs, files)

    assert set(new_dirs) == {"dir1", "dir2"}
    assert set(new_files) == {"file1", "file2", omit_dir}


def test_ignore_complex(tmp_dir, dvc):
    from dvc.fs import localfs

    spec = """\
# Ignore everything
1/**
# Except directories (leaves all files ignored)
!1/**/
# Don't ignore files in 3
!seq/**/3/**

data/
!data/keep.csv

data2/**
!data2/**/
!data2/**/*.csv

ignore.txt
!no-ignore.txt
"""
    (tmp_dir / ".dvcignore").write_text(spec)
    (tmp_dir / "1" / "2" / "3").mkdir(parents=True, exist_ok=True)
    (tmp_dir / "1" / "2" / "shouldIgnore.txt").touch()
    (tmp_dir / "1" / "2" / "3" / "shouldKeep.txt").touch()
    (tmp_dir / "data" / "subdir").mkdir(parents=True, exist_ok=True)
    (tmp_dir / "data2" / "subdir").mkdir(parents=True, exist_ok=True)
    (tmp_dir / "data" / "keep.csv").touch()
    (tmp_dir / "data" / "other.csv").touch()
    (tmp_dir / "data" / "subdir" / "file.txt").touch()
    (tmp_dir / "data2" / "keep.csv").touch()
    (tmp_dir / "data2" / "other.txt").touch()
    (tmp_dir / "data2" / "subdir" / "keep.csv").touch()
    (tmp_dir / "data2" / "subdir" / "other.txt").touch()
    (tmp_dir / "ignore.txt").touch()
    (tmp_dir / "no-ignore.txt").touch()

    ignore_file = DvcIgnorePatterns.from_file(
        os.fspath(tmp_dir / ".dvcignore"), localfs, ".dvcignore"
    )
    dvc.__dict__.pop("dvcignore", None)

    def matches(path):
        result, _matches = ignore_file.matches(
            os.fspath(tmp_dir), path, (tmp_dir / path).is_dir(), details=True
        )
        return result, [str(m) for m in _matches]

    for path, *expected in [
        ("1", False, [".dvcignore:4:!1/**/"]),
        (join("1", ""), False, [".dvcignore:4:!1/**/"]),
        (join("1", "2"), False, [".dvcignore:4:!1/**/"]),
        (join("1", "2", ""), False, [".dvcignore:4:!1/**/"]),
        (join("1", "2", "shouldIgnore.txt"), True, [".dvcignore:2:1/**"]),
        (join("1", "2", "3"), False, [".dvcignore:4:!1/**/"]),
        (join("1", "2", "3", ""), False, [".dvcignore:4:!1/**/"]),
        (join("1", "2", "3", "shouldKeep.txt"), True, [".dvcignore:2:1/**"]),
        ("data", True, [".dvcignore:8:data/"]),
        (join("data", ""), True, [".dvcignore:8:data/"]),
        (join("data", "keep.csv"), True, [".dvcignore:8:data/"]),
        (join("data", "other.csv"), True, [".dvcignore:8:data/"]),
        (join("data", "subdir", "file.txt"), True, [".dvcignore:8:data/"]),
        ("data2", False, [".dvcignore:12:!data2/**/"]),
        (join("data2", ""), False, [".dvcignore:12:!data2/**/"]),
        (join("data2", "keep.csv"), False, [".dvcignore:13:!data2/**/*.csv"]),
        (join("data2", "other.txt"), True, [".dvcignore:11:data2/**"]),
        (join("data2", "subdir"), False, [".dvcignore:12:!data2/**/"]),
        (join("data2", "subdir", ""), False, [".dvcignore:12:!data2/**/"]),
        (join("data2", "subdir", "keep.csv"), False, [".dvcignore:13:!data2/**/*.csv"]),
        (join("data2", "subdir", "other.txt"), True, [".dvcignore:11:data2/**"]),
        ("ignore.txt", True, [".dvcignore:15:ignore.txt"]),
        ("no-ignore.txt", False, [".dvcignore:16:!no-ignore.txt"]),
    ]:
        assert matches(path) == tuple(expected), f"for {path}"

    def sorted_walk(path):
        for root, dirs, files in dvc.dvcignore.walk(localfs, path):
            dirs.sort()
            files.sort()
            yield root, dirs, files

    assert list(sorted_walk(os.curdir)) == [
        (
            os.curdir,
            ["1", "data2"],
            [".dvcignore", "no-ignore.txt"],
        ),
        ("1", ["2"], []),
        (join("1", "2"), ["3"], []),
        (join("1", "2", "3"), [], []),
        ("data2", ["subdir"], ["keep.csv"]),
        (join("data2", "subdir"), [], ["keep.csv"]),
    ]


def test_ignore_unignore_from_git_example(tmp_dir, dvc, scm):
    from dvc.fs import localfs

    spec = """\
# exclude everything except directory foo/bar
/*
!/foo
/foo/*
!/foo/bar
"""
    (tmp_dir / ".dvcignore").write_text(spec)
    for d in [
        tmp_dir,
        tmp_dir / "foo",
        tmp_dir / "foo" / "bar",
        tmp_dir / "foo" / "baz",
        tmp_dir / "foobar",
    ]:
        d.mkdir(parents=True, exist_ok=True)
        (d / "myfile").touch()

    ignore_file = DvcIgnorePatterns.from_file(
        os.fspath(tmp_dir / ".dvcignore"), localfs, ".dvcignore"
    )
    dvc.__dict__.pop("dvcignore", None)

    def matches(path):
        result, _matches = ignore_file.matches(
            os.fspath(tmp_dir), path, (tmp_dir / path).is_dir(), details=True
        )
        return result, [str(m) for m in _matches]

    for path, *expected in [
        ("foo", False, [".dvcignore:3:!/foo"]),
        (join("foo", ""), False, [".dvcignore:3:!/foo"]),
        (join("foo", "myfile"), True, [".dvcignore:4:/foo/*"]),
        (join("foo", "bar"), False, [".dvcignore:5:!/foo/bar"]),
        (join("foo", "bar", ""), False, [".dvcignore:5:!/foo/bar"]),
        # matching pattern differs from git for foo/bar/myfile
        (join("foo", "bar", "myfile"), False, [".dvcignore:5:!/foo/bar"]),
        (join("foo", "baz"), True, [".dvcignore:4:/foo/*"]),
        (join("foo", "baz", ""), True, [".dvcignore:4:/foo/*"]),
        (join("foo", "baz", "myfile"), True, [".dvcignore:4:/foo/*"]),
        (join("foo", "foobar"), True, [".dvcignore:4:/foo/*"]),
        (join("foo", "foobar", ""), True, [".dvcignore:4:/foo/*"]),
        (join("foo", "foobar", "myfile"), True, [".dvcignore:4:/foo/*"]),
    ]:
        assert matches(path) == tuple(expected), f"for {path}"

    assert sorted(dvc.dvcignore.walk(localfs, os.curdir), key=lambda r: r[0]) == [
        (os.curdir, ["foo"], []),
        ("foo", ["bar"], []),
        (join("foo", "bar"), [], ["myfile"]),
    ]
