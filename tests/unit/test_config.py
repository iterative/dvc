import os

import pytest

from dvc.config import Config
from dvc.tree.local import LocalTree


@pytest.mark.parametrize(
    "path, expected",
    [
        ("cache", "../cache"),
        (os.path.join("..", "cache"), "../../cache"),
        (os.getcwd(), os.getcwd().replace("\\", "/")),
        ("ssh://some/path", "ssh://some/path"),
    ],
)
def test_to_relpath(path, expected):
    assert Config._to_relpath(os.path.join(".", "config"), path) == expected


def test_get_tree(tmp_dir, scm):
    tmp_dir.scm_gen("foo", "foo", commit="add foo")

    tree = scm.get_tree("master")
    config = Config(tree=tree)

    assert config.tree == tree
    assert config.wtree != tree
    assert isinstance(config.wtree, LocalTree)

    assert config._get_tree("repo") == tree
    assert config._get_tree("local") == config.wtree
    assert config._get_tree("global") == config.wtree
    assert config._get_tree("system") == config.wtree
