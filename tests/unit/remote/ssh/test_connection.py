import filecmp
import os
import platform
import posixpath
import tempfile

import pytest

from dvc.command.version import CmdVersion
from dvc.system import System

here = os.path.abspath(os.path.dirname(__file__))


def test_isdir(ssh_connection):
    assert ssh_connection.isdir(here)
    assert not ssh_connection.isdir(__file__)


def test_exists(ssh_connection):
    assert not ssh_connection.exists("/path/to/non/existent/file")
    assert ssh_connection.exists(__file__)


def test_isfile(ssh_connection):
    assert ssh_connection.isfile(__file__)
    assert not ssh_connection.isfile(here)


def test_makedirs(tmp_path, ssh_connection):
    tmp = tmp_path.absolute().as_posix()
    path = posixpath.join(tmp, "dir", "subdir")
    ssh_connection.makedirs(path)
    assert os.path.isdir(path)


def test_remove_dir(tmp_path, ssh_connection):
    dpath = tmp_path / "dir"
    dpath.mkdir()
    (dpath / "file").write_text("file")
    (dpath / "subdir").mkdir()
    (dpath / "subdir" / "subfile").write_text("subfile")
    ssh_connection.remove(dpath.absolute().as_posix())
    assert not dpath.exists()


def test_walk(tmp_path, ssh_connection):
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
    expected = {entry.absolute().as_posix() for entry in entries}

    paths = set()
    for root, dirs, files in ssh_connection.walk(
        root_path.absolute().as_posix()
    ):
        for entry in dirs + files:
            paths.add(posixpath.join(root, entry))

    assert paths == expected


@pytest.mark.skipif(
    CmdVersion.get_fs_type(tempfile.gettempdir())[0]
    not in ["xfs", "apfs", "btrfs"],
    reason="Reflinks only work in specified file systems",
)
def test_reflink(tmp_dir, ssh_connection):
    tmp_dir.gen("foo", "foo content")
    ssh_connection.reflink("foo", "link")
    assert filecmp.cmp("foo", "link")
    assert not System.is_symlink("link")
    assert not System.is_hardlink("link")


@pytest.mark.skipif(
    platform.system() == "Windows",
    reason="sftp symlink is not supported on Windows",
)
def test_symlink(tmp_dir, ssh_connection):
    tmp_dir.gen("foo", "foo content")
    ssh_connection.symlink("foo", "link")
    assert System.is_symlink("link")


@pytest.mark.skipif(
    platform.system() == "Windows",
    reason="hardlink is temporarily not supported on Windows",
)
def test_hardlink(tmp_dir, ssh_connection):
    tmp_dir.gen("foo", "foo content")
    ssh_connection.hardlink("foo", "link")
    assert System.is_hardlink("link")


@pytest.mark.skipif(
    platform.system() == "Windows",
    reason="copy is temporarily not supported on Windows",
)
def test_copy(tmp_dir, ssh_connection):
    tmp_dir.gen("foo", "foo content")
    ssh_connection.copy("foo", "link")
    assert filecmp.cmp("foo", "link")


def test_move(tmp_dir, ssh_connection):
    tmp_dir.gen("foo", "foo content")
    ssh_connection.move("foo", "copy")
    assert os.path.exists("copy")
    assert not os.path.exists("foo")
