import re

import pytest

from dvc.command.version import psutil
from dvc.main import main


def test_info_in_repo(repo_dir, dvc_repo, caplog):
    # adding a file so that dvc creates `.dvc/cache`, that is needed for proper
    # supported link types check.
    assert main(["add", repo_dir.FOO]) == 0
    assert main(["version"]) == 0

    assert re.search(re.compile(r"DVC version: \d+\.\d+\.\d+"), caplog.text)
    assert re.search(re.compile(r"Python version: \d\.\d\.\d"), caplog.text)
    assert re.search(re.compile(r"Platform: .*"), caplog.text)
    assert re.search(re.compile(r"Binary: (True|False)"), caplog.text)
    assert re.search(re.compile(r"Package: .*"), caplog.text)
    assert re.search(
        re.compile(r"(Cache: (.*link - (True|False)(,\s)?){3})"), caplog.text
    )


@pytest.mark.skipif(psutil is None, reason="No psutil.")
def test_fs_info_in_repo(dvc_repo, caplog):
    assert main(["version"]) == 0

    assert re.search(
        re.compile(r"Filesystem type \(cache directory\): .*"), caplog.text
    )
    assert re.search(
        re.compile(r"Filesystem type \(workspace\): .*"), caplog.text
    )


def test_info_outside_of_repo(repo_dir, caplog):
    assert main(["version"]) == 0

    assert re.search(re.compile(r"DVC version: \d+\.\d+\.\d+"), caplog.text)
    assert re.search(re.compile(r"Python version: \d\.\d\.\d"), caplog.text)
    assert re.search(re.compile(r"Platform: .*"), caplog.text)
    assert re.search(re.compile(r"Binary: (True|False)"), caplog.text)
    assert re.search(re.compile(r"Package: .*"), caplog.text)
    assert not re.search(
        re.compile(r"(Cache: (.*link - (True|False)(,\s)?){3})"), caplog.text
    )


@pytest.mark.skipif(psutil is None, reason="No psutil.")
def test_fs_info_outside_of_repo(repo_dir, caplog):
    assert main(["version"]) == 0

    assert re.search(
        re.compile(r"Filesystem type \(workspace\): .*"), caplog.text
    )
    assert not re.search(
        re.compile(r"Filesystem type \(cache directory\): .*"), caplog.text
    )
