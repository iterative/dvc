import os

import pytest
from dvc.ignore import DvcIgnoreFromFile, DvcIgnoreDir, DvcIgnoreFile
from mock import patch, Mock

from dvc.utils.compat import cast_bytes


def read_pattern(p):
    return cast_bytes(p, "utf-8")


def mock_dvcignore(dvcignore_path, negate_patterns, patterns):
    negate_patterns = [read_pattern(p) for p in negate_patterns]
    patterns = [read_pattern(p) for p in patterns]

    mock_ignore_file_handler = Mock()
    with patch.object(
        mock_ignore_file_handler,
        "read_patterns",
        return_value=(negate_patterns, patterns),
    ):
        ignore_file = DvcIgnoreFromFile(
            dvcignore_path, mock_ignore_file_handler
        )
    return ignore_file


def test_ignore_from_file_should_filter_dirs_and_files():
    dvcignore_path = os.path.join(
        os.path.sep, "full", "path", "to", "ignore", "file", ".dvcignore"
    )

    negate_patterns = []
    patterns = ["dir_to_ignore", "file_to_ignore"]

    root = os.path.dirname(dvcignore_path)
    dirs = ["dir1", "dir2", "dir_to_ignore"]
    files = ["file1", "file2", "file_to_ignore"]

    ignore = mock_dvcignore(dvcignore_path, negate_patterns, patterns)
    new_dirs, new_files = ignore(root, dirs, files)

    assert {"dir1", "dir2"} == set(new_dirs)
    assert {"file1", "file2"} == set(new_files)


@pytest.mark.parametrize(
    "file_to_ignore_relpath, negate_patterns,patterns,  expected_match",
    [
        ("to_ignore", [], ["to_ignore"], True),
        ("to_ignore.txt", [], ["to_ignore*"], True),
        (
            os.path.join("rel", "p", "p2", "to_ignore"),
            [],
            ["rel/**/to_ignore"],
            True,
        ),
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
            [],
            ["to_ignore"],
            True,
        ),
        ("to_ignore.txt", [], ["/*.txt"], True),
        (
            os.path.join("rel", "path", "path2", "to_ignore"),
            [],
            ["rel/*/to_ignore"],
            False,
        ),
        (os.path.join("path", "to_ignore.txt"), [], ["/*.txt"], False),
        (
            os.path.join("rel", "path", "path2", "dont_ignore"),
            [],
            ["rel/**/to_ignore"],
            False,
        ),
        ("dont_ignore.txt", [], ["dont_ignore"], False),
        ("dont_ignore.txt", ["!dont_ignore.txt"], ["dont*"], False),
    ],
)
def test_match_ignore_from_file(
    file_to_ignore_relpath, negate_patterns, patterns, expected_match
):

    dvcignore_path = os.path.join(
        os.path.sep, "full", "path", "to", "ignore", "file", ".dvcignore"
    )
    dvcignore_dirname = os.path.dirname(dvcignore_path)

    ignore_file = mock_dvcignore(dvcignore_path, negate_patterns, patterns)

    assert (
        ignore_file.matches(dvcignore_dirname, file_to_ignore_relpath)
        == expected_match
    )


@pytest.mark.parametrize("omit_dir", [".git", ".hg", ".dvc"])
def test_should_ignore_dir(omit_dir):
    ignore = DvcIgnoreDir(omit_dir)

    root = os.path.join(os.path.sep, "walk", "dir", "root")
    dirs = [omit_dir, "dir1", "dir2"]
    files = []

    new_dirs, _ = ignore(root, dirs, files)

    assert set(new_dirs) == {"dir1", "dir2"}


def test_should_ignore_file():
    dvcignore = ".dvcignore"
    ignore = DvcIgnoreFile(dvcignore)

    root = os.path.join(os.path.sep, "walk", "dir", "root")
    dirs = []
    files = ["file1", "file2", dvcignore]

    _, new_files = ignore(root, dirs, files)

    assert set(new_files) == {"file1", "file2"}
