import os
import re
import shutil

import pytest

from dvc.info import get_dvc_info

# Python's version is in the shape of:
# <major>.<minor>.<patch>[{a|b|rc}N][.postN][.devN]
# `patch` is more than enough for the tests.
# Refer PEP-0440 for complete regex just in-case.
PYTHON_VERSION_REGEX = r"Python \d\.\d+\.\d+\S*"
DVC_VERSION_REGEX = r"\d+\.\d+\.(\d+\.)?.*"


def find_supported_remotes(string):
    lines = string.splitlines()
    index = 0

    for index, line in enumerate(lines):
        if line == "Supports:":
            index += 1
            break
    else:
        return []

    remotes = {}
    for line in lines[index:]:
        if not line.startswith("\t"):
            break

        remote_name, _, raw_dependencies = (
            line.strip().strip(",").partition(" ")
        )
        remotes[remote_name] = {
            dependency: version
            for dependency, _, version in [
                dependency.partition(" = ")
                for dependency in raw_dependencies[1:-1].split(", ")
            ]
        }
    return remotes


@pytest.mark.parametrize("scm_init", [True, False])
def test_info_in_repo(scm_init, tmp_dir):
    tmp_dir.init(scm=scm_init, dvc=True)
    # Create `.dvc/cache`, that is needed to check supported link types.
    os.mkdir(tmp_dir.dvc.odb.local.cache_dir)

    dvc_info = get_dvc_info()

    assert re.search(rf"DVC version: {DVC_VERSION_REGEX}", dvc_info)
    assert re.search(f"Platform: {PYTHON_VERSION_REGEX} on .*", dvc_info)
    assert find_supported_remotes(dvc_info)
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


def test_caches(tmp_dir, dvc, caplog):
    tmp_dir.add_remote(
        name="sshcache", url="ssh://example.com/path", default=False
    )
    with tmp_dir.dvc.config.edit() as conf:
        conf["cache"]["ssh"] = "sshcache"

    dvc_info = get_dvc_info()

    # Order of cache types is runtime dependent
    assert re.search("Caches: (local, ssh|ssh, local)", dvc_info)


def test_remotes_empty(tmp_dir, dvc, caplog):
    # No remotes are configured
    dvc_info = get_dvc_info()

    assert "Remotes: None" in dvc_info


def test_remotes(tmp_dir, dvc, caplog):
    tmp_dir.add_remote(name="server", url="ssh://localhost", default=False)
    tmp_dir.add_remote(
        name="r1", url="azure://example.com/path", default=False
    )
    tmp_dir.add_remote(name="r2", url="remote://server/path", default=False)

    dvc_info = get_dvc_info()

    assert re.search("Remotes: (ssh, azure|azure, ssh)", dvc_info)


def test_fs_info_in_repo(tmp_dir, dvc, caplog):
    os.mkdir(dvc.odb.local.cache_dir)
    dvc_info = get_dvc_info()

    assert re.search(r"Cache directory: .* on .*", dvc_info)
    assert re.search(r"Workspace directory: .* on .*", dvc_info)


def test_info_outside_of_repo(tmp_dir, caplog):
    dvc_info = get_dvc_info()

    assert re.search(rf"DVC version: {DVC_VERSION_REGEX}", dvc_info)
    assert re.search(f"Platform: {PYTHON_VERSION_REGEX} on .*", dvc_info)
    assert find_supported_remotes(dvc_info)
    assert not re.search(r"Cache types: .*", dvc_info)
    assert "Repo:" not in dvc_info


def test_fs_info_outside_of_repo(tmp_dir, caplog):
    dvc_info = get_dvc_info()
    assert re.search(rf"DVC version: {DVC_VERSION_REGEX}", dvc_info)
    assert re.search(f"Platform: {PYTHON_VERSION_REGEX} on .*", dvc_info)
    assert find_supported_remotes(dvc_info)


def test_plugin_versions(tmp_dir, dvc):
    from dvc.fs import FS_MAP

    dvc_info = get_dvc_info()
    remotes = find_supported_remotes(dvc_info)

    for remote, dependencies in remotes.items():
        assert dependencies.keys() == FS_MAP[remote].REQUIRES.keys()
