import os

import pytest

from dvc.exceptions import URLMissingError
from dvc.repo import Repo


def match_files(entries, expected):
    entries_content = {(d["path"], d["isdir"]) for d in entries}
    expected_content = {(d["path"], d["isdir"]) for d in expected}
    assert entries_content == expected_content


def test_file(tmp_dir):
    tmp_dir.gen({"foo": "foo contents"})

    result = Repo.ls_url("foo")
    match_files(result, [{"path": os.path.abspath("foo"), "isdir": False}])


def test_dir(tmp_dir):
    tmp_dir.gen({"dir/foo": "foo contents", "dir/subdir/bar": "bar contents"})

    result = Repo.ls_url("dir")
    match_files(
        result,
        [{"path": "foo", "isdir": False}, {"path": "subdir", "isdir": True}],
    )


def test_nonexistent(tmp_dir):
    with pytest.raises(URLMissingError):
        Repo.ls_url("nonexistent")
