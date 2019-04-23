from __future__ import unicode_literals

import mockssh
import os
import posixpath
import pytest

from dvc.remote.ssh.connection import SSHConnection

here = os.path.abspath(os.path.dirname(__file__))

user = "user"
key_path = os.path.join(here, "{0}.key".format(user))


@pytest.yield_fixture()
def ssh():
    users = {user: key_path}
    with mockssh.Server(users) as s:
        yield SSHConnection(
            s.host, username=user, port=s.port, key_filename=key_path
        )


def test_connection(ssh):
    assert ssh.execute("ls /")


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
