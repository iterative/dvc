import pytest
import copy

from dvc.path_info import URLInfo, CloudURLInfo


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


@pytest.mark.parametrize("cls", [URLInfo, CloudURLInfo])
def test_url_info_deepcopy(cls):
    u1 = cls("ssh://user@test.com:/test1/test2/test3")
    u2 = copy.deepcopy(u1)
    assert u1 == u2
