import re

from dvc.cli import main
from tests.unit.test_info import (
    DVC_VERSION_REGEX,
    PYTHON_VERSION_REGEX,
    SUBPROJECTS,
    find_supported_remotes,
)


def test_(tmp_dir, dvc, scm, capsys):
    assert main(["version"]) == 0

    out, _ = capsys.readouterr()
    assert re.search(rf"DVC version: {DVC_VERSION_REGEX}", out)
    assert re.search(f"Platform: {PYTHON_VERSION_REGEX} on .*", out)
    for subproject in SUBPROJECTS:
        assert re.search(rf"{subproject} = .*", out)

    assert find_supported_remotes(out)
    assert re.search(r"Cache types: .*", out)
    assert re.search(r"Caches: local", out)
    assert re.search(r"Remotes: None", out)
    assert "Repo: dvc, git" in out


def test_import_error(tmp_dir, dvc, scm, capsys, monkeypatch):
    import importlib.metadata as importlib_metadata

    original = importlib_metadata.version

    def _import_error(name):
        if name == "dvclive":
            raise ImportError
        return original(name)

    monkeypatch.setattr(importlib_metadata, "version", _import_error)
    assert main(["version"]) == 0

    out, _ = capsys.readouterr()

    for subproject in SUBPROJECTS:
        match = re.search(rf"{subproject} = {DVC_VERSION_REGEX}", out)
        if subproject != "dvclive":
            assert match
        else:
            assert match is None
