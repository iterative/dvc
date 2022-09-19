import os

import pytest

from dvc.exceptions import URLMissingError
from dvc.repo import Repo


def match_files(entries, expected):
    entries_content = {(d["path"], d["isdir"]) for d in entries}
    expected_content = {(d["path"], d["isdir"]) for d in expected}
    assert entries_content == expected_content


@pytest.mark.parametrize("fname", ["foo", "dir/foo"])
def test_file(tmp_dir, fname):
    tmp_dir.gen({fname: "foo contents"})

    (d,) = Repo.ls_url(fname)
    assert d["isdir"] is False
    assert os.path.normpath(d["path"]) == os.path.abspath(fname)


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
