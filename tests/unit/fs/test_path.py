import pytest

from dvc.fs import Path


@pytest.mark.parametrize("prefix", ["", "/"])
@pytest.mark.parametrize("postfix", ["", "/"])
@pytest.mark.parametrize(
    "path,expected",
    [
        ("path", ("path",)),
        ("some/path", ("some", "path")),
    ],
)
def test_parts_posix(prefix, postfix, path, expected):
    assert Path("/").parts(prefix + path + postfix) == tuple(prefix) + expected


@pytest.mark.parametrize("postfix", ["", "\\"])
@pytest.mark.parametrize(
    "path,expected",
    [
        ("path", ("path",)),
        ("c:\\path", ("c:", "\\", "path")),
        ("some\\path", ("some", "path")),
    ],
)
def test_parts_nt(postfix, path, expected):
    assert Path("\\").parts(path + postfix) == expected
