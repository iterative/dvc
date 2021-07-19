import re

from dvc.main import main
from tests.unit.test_info import PYTHON_VERSION_REGEX, find_supported_remotes


def test_(tmp_dir, dvc, scm, capsys):
    assert main(["version"]) == 0

    out, _ = capsys.readouterr()
    assert re.search(r"DVC version: \d+\.\d+\.\d+.*", out)
    assert re.search(f"Platform: {PYTHON_VERSION_REGEX} on .*", out)
    assert find_supported_remotes(out)
    assert re.search(r"Cache types: .*", out)
    assert re.search(r"Caches: local", out)
    assert re.search(r"Remotes: None", out)
    assert "Repo: dvc, git" in out
