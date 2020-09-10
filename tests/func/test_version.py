import re

from dvc.main import main


def test_(tmp_dir, dvc, scm, caplog):
    assert main(["version"]) == 0

    assert re.search(r"DVC version: \d+\.\d+\.\d+.*", caplog.text)
    assert re.search(r"Platform: Python \d\.\d+\.\d+ on .*", caplog.text)
    assert re.search(r"Supports: .*", caplog.text)
    assert re.search(r"Cache types: .*", caplog.text)
    assert "Repo: dvc, git" in caplog.text
