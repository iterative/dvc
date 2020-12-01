import pytest

from dvc.tree.base import BaseTree, RemoteMissingDepsError


@pytest.mark.parametrize(
    "pkg, msg",
    [
        (None, "Please report this bug to"),
        ("pip", "pip install"),
        ("conda", "conda install"),
    ],
)
def test_missing_deps(pkg, msg, mocker):
    requires = {"missing": "missing"}
    mocker.patch.object(BaseTree, "REQUIRES", requires)
    mocker.patch("dvc.utils.pkg.PKG", pkg)
    with pytest.raises(RemoteMissingDepsError, match=msg):
        BaseTree(None, {})
