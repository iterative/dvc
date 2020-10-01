import os
import re
import shutil

import pytest

from dvc.info import get_dvc_info, psutil

# Python's version is in the shape of:
# <major>.<minor>.<patch>[{a|b|rc}N][.postN][.devN]
# `patch` is more than enough for the tests.
# Refer PEP-0440 for complete regex just in-case.
PYTHON_VERSION_REGEX = r"Python \d\.\d+\.\d+\S*"


@pytest.mark.parametrize("scm_init", [True, False])
def test_info_in_repo(scm_init, tmp_dir):
    tmp_dir.init(scm=scm_init, dvc=True)
    # Create `.dvc/cache`, that is needed to check supported link types.
    os.mkdir(tmp_dir.dvc.cache.local.cache_dir)

    dvc_info = get_dvc_info()

    assert re.search(r"DVC version: \d+\.\d+\.\d+.*", dvc_info)
    assert re.search(f"Platform: {PYTHON_VERSION_REGEX} on .*", dvc_info)
    assert re.search(r"Supports: .*", dvc_info)
    assert re.search(r"Cache types: .*", dvc_info)

    if scm_init:
        assert "Repo: dvc, git" in dvc_info
    else:
        assert "Repo: dvc (no_scm)" in dvc_info


def test_info_in_subdir(tmp_dir, scm, caplog):
    dvc_subdir = tmp_dir / "subdir"
    dvc_subdir.mkdir()

    with dvc_subdir.chdir():
        dvc_subdir.init(scm=False, dvc=True)
        with dvc_subdir.dvc.config.edit() as conf:
            del conf["core"]["no_scm"]

        dvc_info = get_dvc_info()

    assert "Repo: dvc (subdir), git" in dvc_info


def test_info_in_broken_git_repo(tmp_dir, dvc, scm, caplog):
    shutil.rmtree(dvc.scm.dir)
    dvc_info = get_dvc_info()

    assert "Repo: dvc, git (broken)" in dvc_info


@pytest.mark.skipif(psutil is None, reason="No psutil.")
def test_fs_info_in_repo(tmp_dir, dvc, caplog):
    os.mkdir(dvc.cache.local.cache_dir)
    dvc_info = get_dvc_info()

    assert re.search(r"Cache directory: .* on .*", dvc_info)
    assert re.search(r"Workspace directory: .* on .*", dvc_info)


def test_info_outside_of_repo(tmp_dir, caplog):
    dvc_info = get_dvc_info()

    assert re.search(r"DVC version: \d+\.\d+\.\d+.*", dvc_info)
    assert re.search(f"Platform: {PYTHON_VERSION_REGEX} on .*", dvc_info)
    assert re.search(r"Supports: .*", dvc_info)
    assert not re.search(r"Cache types: .*", dvc_info)
    assert "Repo:" not in dvc_info


@pytest.mark.skipif(psutil is None, reason="No psutil.")
def test_fs_info_outside_of_repo(tmp_dir, caplog):
    dvc_info = get_dvc_info()
    assert re.search(r"DVC version: \d+\.\d+\.\d+.*", dvc_info)
    assert re.search(f"Platform: {PYTHON_VERSION_REGEX} on .*", dvc_info)
    assert re.search(r"Supports: .*", dvc_info)
