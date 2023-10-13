from dvc.config import Config
from dvc.repo import Repo
from dvc.testing.workspace_tests import TestLsUrl as _TestLsUrl


class TestLsUrl(_TestLsUrl):
    pass


def test_ls_url_config(dvc, make_remote):
    remote_path = make_remote("myremote", default=False, typ="local")
    (remote_path / "foo").write_text("foo")
    (remote_path / "bar").write_text("bar")

    actual = sorted(
        Repo.ls_url("remote://myremote", config=Config.from_cwd()),
        key=lambda entry: entry["path"],
    )
    expected = [
        {"isdir": False, "path": "bar", "size": 3},
        {"isdir": False, "path": "foo", "size": 3},
    ]
    assert actual == expected
