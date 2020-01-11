import os
import re

import pytest

from dvc.command.version import psutil
from dvc.main import main


def test_info_in_repo(tmp_dir, dvc, caplog):
    # Create `.dvc/cache`, that is needed to check supported link types.
    os.mkdir(dvc.cache.local.cache_dir)
    assert main(["version"]) == 0

    assert re.search(r"DVC version: \d+\.\d+\.\d+", caplog.text)
    assert re.search(r"Python version: \d\.\d\.\d", caplog.text)
    assert re.search(r"Platform: .*", caplog.text)
    assert re.search(r"Binary: (True|False)", caplog.text)
    assert re.search(r"Package: .*", caplog.text)
    assert re.search(
        r"(Cache: (.*link - (not )?supported(,\s)?){3})", caplog.text
    )


@pytest.mark.skipif(psutil is None, reason="No psutil.")
def test_fs_info_in_repo(tmp_dir, dvc, caplog):
    os.mkdir(dvc.cache.local.cache_dir)
    assert main(["version"]) == 0

    assert "Filesystem type (cache directory): " in caplog.text
    assert "Filesystem type (workspace): " in caplog.text


def test_info_outside_of_repo(tmp_dir, caplog):
    assert main(["version"]) == 0

    assert re.search(r"DVC version: \d+\.\d+\.\d+", caplog.text)
    assert re.search(r"Python version: \d\.\d\.\d", caplog.text)
    assert re.search(r"Platform: .*", caplog.text)
    assert re.search(r"Binary: (True|False)", caplog.text)
    assert re.search(r"Package: .*", caplog.text)
    assert not re.search(r"(Cache: (.*link - (not )?(,\s)?){3})", caplog.text)


@pytest.mark.skipif(psutil is None, reason="No psutil.")
def test_fs_info_outside_of_repo(tmp_dir, caplog):
    assert main(["version"]) == 0

    assert "Filesystem type (cache directory): " not in caplog.text
    assert "Filesystem type (workspace): " in caplog.text
