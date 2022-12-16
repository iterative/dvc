import pytest

from dvc.fs import FileSystem, RemoteMissingDepsError


def test_missing_deps(mocker):
    requires = {"missing": "missing"}
    mocker.patch.object(FileSystem, "REQUIRES", requires)
    with pytest.raises(RemoteMissingDepsError, match="missing dependencies"):
        FileSystem()
