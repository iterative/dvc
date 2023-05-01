import errno
import os

import pytest

from dvc.exceptions import FileExistsLocallyError, URLMissingError
from dvc.repo import Repo
from dvc.testing.workspace_tests import TestGetUrl as _TestGetUrl


def test_get_file(tmp_dir):
    tmp_dir.gen({"foo": "foo contents"})

    Repo.get_url("foo", "foo_imported")

    assert (tmp_dir / "foo_imported").is_file()
    assert (tmp_dir / "foo_imported").read_text() == "foo contents"


def test_get_file_conflict_and_override(tmp_dir):
    tmp_dir.gen({"foo": "foo contents"})
    tmp_dir.gen({"bar": "bar contents"})

    with pytest.raises(FileExistsLocallyError) as exc_info:
        Repo.get_url("foo", "bar")

    # verify no override
    assert (tmp_dir / "bar").is_file()
    assert (tmp_dir / "bar").read_text() == "bar contents"

    # verify meaningful/BC exception type/errno
    assert isinstance(exc_info.value, FileExistsError)
    assert exc_info.value.errno == errno.EEXIST

    # now, override
    Repo.get_url("foo", "bar", force=True)

    assert (tmp_dir / "bar").is_file()
    assert (tmp_dir / "bar").read_text() == "foo contents"


def test_get_dir(tmp_dir):
    tmp_dir.gen({"foo": {"foo": "foo contents"}})

    Repo.get_url("foo", "foo_imported")

    assert (tmp_dir / "foo_imported").is_dir()
    assert (tmp_dir / "foo_imported" / "foo").is_file()
    assert (tmp_dir / "foo_imported" / "foo").read_text() == "foo contents"


@pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
def test_get_url_to_dir(tmp_dir, dname):
    tmp_dir.gen({"src": {"foo": "foo contents"}, "dir": {"subdir": {}}})

    Repo.get_url(os.path.join("src", "foo"), dname)

    assert (tmp_dir / dname).is_dir()
    assert (tmp_dir / dname / "foo").read_text() == "foo contents"


def test_get_url_nonexistent(tmp_dir):
    with pytest.raises(URLMissingError):
        Repo.get_url("nonexistent")


class TestGetUrl(_TestGetUrl):
    pass
