import os
import re
import shutil

import pytest

from dvc.command.version import psutil
from dvc.main import main


@pytest.mark.parametrize("scm_init", [True, False])
def test_info_in_repo(scm_init, tmp_dir, caplog):
    tmp_dir.init(scm=scm_init, dvc=True)
    # Create `.dvc/cache`, that is needed to check supported link types.
    os.mkdir(tmp_dir.dvc.cache.local.cache_dir)

    assert main(["version"]) == 0

    assert re.search(r"DVC version: \d+\.\d+\.\d+.*", caplog.text)
    assert re.search(r"Platform: Python \d\.\d+\.\d+ on .*", caplog.text)
    assert re.search(r"Supports: .*", caplog.text)
    assert re.search(r"Cache types: .*", caplog.text)

    if scm_init:
        assert "Repo: dvc, git" in caplog.text
    else:
        assert "Repo: dvc (no_scm)" in caplog.text


def test_info_in_subdir(tmp_dir, scm, caplog):
    dvc_subdir = tmp_dir / "subdir"
    dvc_subdir.mkdir()

    with dvc_subdir.chdir():
        dvc_subdir.init(scm=False, dvc=True)
        with dvc_subdir.dvc.config.edit() as conf:
            del conf["core"]["no_scm"]

        assert main(["version"]) == 0

    assert "Repo: dvc (subdir), git" in caplog.text


def test_info_in_broken_git_repo(tmp_dir, dvc, scm, caplog):
    shutil.rmtree(dvc.scm.dir)
    assert main(["version"]) == 0

    assert "Repo: dvc, git (broken)" in caplog.text


@pytest.mark.skipif(psutil is None, reason="No psutil.")
def test_fs_info_in_repo(tmp_dir, dvc, caplog):
    os.mkdir(dvc.cache.local.cache_dir)
    assert main(["version"]) == 0

    assert re.search(r"Cache directory: .* on .*", caplog.text)
    assert re.search(r"Workspace directory: .* on .*", caplog.text)


def test_info_outside_of_repo(tmp_dir, caplog):
    assert main(["version"]) == 0

    assert re.search(r"DVC version: \d+\.\d+\.\d+.*", caplog.text)
    assert re.search(r"Platform: Python \d\.\d+\.\d+ on .*", caplog.text)
    assert re.search(r"Supports: .*", caplog.text)
    assert not re.search(r"Cache types: .*", caplog.text)
    assert "Repo:" not in caplog.text


@pytest.mark.skipif(psutil is None, reason="No psutil.")
def test_fs_info_outside_of_repo(tmp_dir, caplog):
    assert main(["version"]) == 0
