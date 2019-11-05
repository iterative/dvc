from __future__ import unicode_literals

import filecmp
import os
import platform
import posixpath
import tempfile

import pytest

from dvc.command.version import CmdVersion
from dvc.system import System


here = os.path.abspath(os.path.dirname(__file__))


def test_isdir(ssh):
    assert ssh.isdir(here)
    assert not ssh.isdir(__file__)


def test_exists(ssh):
    assert not ssh.exists("/path/to/non/existent/file")
    assert ssh.exists(__file__)


def test_isfile(ssh):
    assert ssh.isfile(__file__)
    assert not ssh.isfile(here)


def test_makedirs(tmp_path, ssh):
    tmp = tmp_path.absolute().as_posix()
    path = posixpath.join(tmp, "dir", "subdir")
    ssh.makedirs(path)
    assert os.path.isdir(path)


def test_walk(tmp_path, ssh):
    root_path = tmp_path
    dir_path = root_path / "dir"
    subdir_path = dir_path / "subdir"

    dir_path.mkdir()
    subdir_path.mkdir()

    root_data_path = root_path / "root_data"
    dir_data_path = dir_path / "dir_data"
    subdir_data_path = subdir_path / "subdir_data"

    with root_data_path.open("w+") as fobj:
        fobj.write("")

    with dir_data_path.open("w+") as fobj:
        fobj.write("")

    with subdir_data_path.open("w+") as fobj:
        fobj.write("")

    entries = [
        dir_path,
        subdir_path,
        root_data_path,
        dir_data_path,
        subdir_data_path,
    ]
    expected = set([entry.absolute().as_posix() for entry in entries])

    paths = set()
    for root, dirs, files in ssh.walk(root_path.absolute().as_posix()):
        for entry in dirs + files:
            paths.add(posixpath.join(root, entry))

    assert paths == expected


@pytest.mark.skipif(
    CmdVersion.get_fs_type(tempfile.gettempdir())[0]
    not in ["xfs", "apfs", "btrfs"],
    reason="Reflinks only work in specified file systems",
)
def test_reflink(repo_dir, ssh):
    ssh.reflink("foo", "link")
    assert filecmp.cmp("foo", "link")
    assert not System.is_symlink("link")
    assert not System.is_hardlink("link")


@pytest.mark.skipif(
    platform.system() == "Windows",
    reason="sftp symlink is not supported on Windows",
)
def test_symlink(repo_dir, ssh):
    ssh.symlink("foo", "link")
    assert System.is_symlink("link")


@pytest.mark.skipif(
    platform.system() == "Windows",
    reason="hardlink is temporarily not supported on Windows",
)
def test_hardlink(repo_dir, ssh):
    ssh.hardlink("foo", "link")
    assert System.is_hardlink("link")


@pytest.mark.skipif(
    platform.system() == "Windows",
    reason="copy is temporarily not supported on Windows",
)
def test_copy(repo_dir, ssh):
    ssh.copy("foo", "link")
    assert filecmp.cmp("foo", "link")


def test_move(repo_dir, ssh):
    ssh.move("foo", "copy")
    assert os.path.exists("copy")
    assert not os.path.exists("foo")
