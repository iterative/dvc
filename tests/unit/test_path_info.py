from dvc.path_info import URLInfo


def test_url_info_str():
    u = URLInfo("ssh://user@test.com:/test1/")
    assert u.url == "ssh://user@test.com/test1/"
    assert str(u) == u.url


def test_url_info_eq():
    u1 = URLInfo("ssh://user@test.com:/test1/")
    u2 = URLInfo("ssh://user@test.com/test1")
    assert u1 == u2


def test_url_info_parent():
    u1 = URLInfo("ssh://user@test.com:/test1/test2")
    p = u1.parent
    u3 = URLInfo("ssh://user@test.com/test1")
    assert u3 == p
    assert str(p) == "ssh://user@test.com/test1"


def test_url_info_parents():
    u1 = URLInfo("ssh://user@test.com:/test1/test2/test3")
    parents = u1.parents
    assert len(parents) == 3
    assert parents[0] == URLInfo("ssh://user@test.com/test1/test2")
    assert parents[1] == URLInfo("ssh://user@test.com/test1")
    assert parents[2] == URLInfo("ssh://user@test.com/")
