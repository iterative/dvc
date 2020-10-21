import pytest

from dvc.tree.pool import get_connection
from dvc.tree.ssh.connection import SSHConnection
from tests.remotes.ssh import TEST_SSH_KEY_PATH, TEST_SSH_USER


def test_doesnt_swallow_errors(ssh_server):
    class MyError(Exception):
        pass

    with pytest.raises(MyError):
        with get_connection(
            SSHConnection,
            host=ssh_server.host,
            port=ssh_server.port,
            username=TEST_SSH_USER,
            key_filename=TEST_SSH_KEY_PATH,
        ):
            raise MyError


@pytest.mark.parametrize("uname,md5command", [("Linux", "md5sum"), ("Darwin", "md5")])
def test_escapes_filepaths_for_md5_calculation(ssh_server, uname, md5command, mocker):
    path_with_spaces = "Some Path With Spaces"
    escaped_path_with_spaces = "\'Some Path With Spaces\'"
    fake_md5 = "x" * 32

    with get_connection(
        SSHConnection,
        host=ssh_server.host,
        port=ssh_server.port,
        username=TEST_SSH_USER,
        key_filename=TEST_SSH_KEY_PATH,
    ) as connection:
        mocker.patch.object(SSHConnection, "uname", new_callable=mocker.PropertyMock(return_value=uname))
        connection.execute = mocker.Mock(return_value=fake_md5)
        connection.md5(path_with_spaces)
        connection.execute.assert_called_with(f"{md5command} {escaped_path_with_spaces}")


def test_escapes_filepaths_for_copy(ssh_server, mocker):
    src_path_with_spaces = "Path With Spaces"
    escaped_src_path_with_spaces = "\'Path With Spaces\'"
    dest_path_with_spaces = "Other Path With Spaces"
    escaped_dest_path_with_spaces = "\'Other Path With Spaces\'"

    with get_connection(
        SSHConnection,
        host=ssh_server.host,
        port=ssh_server.port,
        username=TEST_SSH_USER,
        key_filename=TEST_SSH_KEY_PATH,
    ) as connection:
        connection.execute = mocker.Mock()
        connection.copy(src_path_with_spaces, dest_path_with_spaces)
        connection.execute.assert_called_with(f"cp {escaped_src_path_with_spaces} {escaped_dest_path_with_spaces}")


@pytest.mark.parametrize("uname,cp_command", [("Linux", "cp --reflink"), ("Darwin", "cp -c")])
def test_escapes_filepaths_for_reflink(ssh_server, uname, cp_command, mocker):
    src_path_with_spaces = "Path With Spaces"
    escaped_src_path_with_spaces = "\'Path With Spaces\'"
    dest_path_with_spaces = "Other Path With Spaces"
    escaped_dest_path_with_spaces = "\'Other Path With Spaces\'"

    with get_connection(
        SSHConnection,
        host=ssh_server.host,
        port=ssh_server.port,
        username=TEST_SSH_USER,
        key_filename=TEST_SSH_KEY_PATH,
    ) as connection:
        mocker.patch.object(SSHConnection, "uname", new_callable=mocker.PropertyMock(return_value=uname))
        connection.execute = mocker.Mock()
        connection.reflink(src_path_with_spaces, dest_path_with_spaces)
        connection.execute.assert_called_with(f"{cp_command} {escaped_src_path_with_spaces} {escaped_dest_path_with_spaces}")


def test_escapes_filepaths_for_hardlink(ssh_server, mocker):
    src_path_with_spaces = "Path With Spaces"
    escaped_src_path_with_spaces = "\'Path With Spaces\'"
    dest_path_with_spaces = "Other Path With Spaces"
    escaped_dest_path_with_spaces = "\'Other Path With Spaces\'"

    with get_connection(
        SSHConnection,
        host=ssh_server.host,
        port=ssh_server.port,
        username=TEST_SSH_USER,
        key_filename=TEST_SSH_KEY_PATH,
    ) as connection:
        connection.execute = mocker.Mock()
        connection.hardlink(src_path_with_spaces, dest_path_with_spaces)
        connection.execute.assert_called_with(f"ln {escaped_src_path_with_spaces} {escaped_dest_path_with_spaces}")