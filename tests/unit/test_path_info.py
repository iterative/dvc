import copy
import pathlib

import pytest

from dvc.path_info import CloudURLInfo, HTTPURLInfo, PathInfo, URLInfo

TEST_DEPTH = len(pathlib.Path(__file__).parents) + 1


@pytest.mark.parametrize("cls", [URLInfo, CloudURLInfo])
def test_url_info_str(cls):
    u = cls("ssh://user@test.com:/test1/")
    assert u.url == "ssh://user@test.com/test1/"
    assert str(u) == u.url


@pytest.mark.parametrize("cls", [URLInfo, CloudURLInfo])
def test_url_info_eq(cls):
    u1 = cls("ssh://user@test.com:/test1/")
    u2 = cls("ssh://user@test.com/test1")
    assert u1 == u2


@pytest.mark.parametrize("cls", [URLInfo, CloudURLInfo])
def test_url_info_parent(cls):
    u1 = cls("ssh://user@test.com:/test1/test2")
    p = u1.parent
    u3 = cls("ssh://user@test.com/test1")
    assert u3 == p
    assert str(p) == "ssh://user@test.com/test1"


@pytest.mark.parametrize("cls", [URLInfo, CloudURLInfo])
def test_url_info_parents(cls):
    u1 = cls("ssh://user@test.com:/test1/test2/test3")
    assert list(u1.parents) == [
        cls("ssh://user@test.com/test1/test2"),
        cls("ssh://user@test.com/test1"),
        cls("ssh://user@test.com/"),
    ]


@pytest.mark.parametrize("cls", [URLInfo, CloudURLInfo, HTTPURLInfo])
def test_url_info_deepcopy(cls):
    u1 = cls("ssh://user@test.com:/test1/test2/test3")
    u2 = copy.deepcopy(u1)
    assert u1 == u2


def test_https_url_info_str():
    url = "https://user@test.com/test1;p=par?q=quer#frag"
    u = HTTPURLInfo(url)
    assert u.url == url
    assert str(u) == u.url
    assert u.params == "p=par"
    assert u.query == "q=quer"
    assert u.fragment == "frag"


@pytest.mark.parametrize(
    "path, as_posix, osname",
    [
        ("/some/abs/path", "/some/abs/path", "posix"),
        ("some/rel/path", "some/rel/path", "posix"),
        ("../some/rel/path", "../some/rel/path", "posix"),
        ("windows\\relpath", "windows/relpath", "nt"),
        ("..\\windows\\rel\\path", "../windows/rel/path", "nt"),
        # These ones are to test that no matter how layered this relpath is,
        # we don't accidentally round it over. E.g. how
        #
        #  import os
        #  os.chdir("/")
        #  os.path.relpath("../../path")
        #
        # results in "path".
        ("\\".join([".."] * TEST_DEPTH), "/".join([".."] * TEST_DEPTH), "nt"),
        (
            "/".join([".."] * TEST_DEPTH),
            "/".join([".."] * TEST_DEPTH),
            "posix",
        ),
    ],
)
def test_path_info_as_posix(mocker, path, as_posix, osname):
    mocker.patch("os.name", osname)
    assert PathInfo(path).as_posix() == as_posix
