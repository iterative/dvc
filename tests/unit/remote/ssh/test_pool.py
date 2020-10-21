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
    return_values = {
        "uname": uname,
        f"{md5command} {path_with_spaces}": fake_md5,
        f"{md5command} {escaped_path_with_spaces}": fake_md5,
    }

    def mock_execute(arg):
        return return_values[arg]

    with get_connection(
        SSHConnection,
        host=ssh_server.host,
        port=ssh_server.port,
        username=TEST_SSH_USER,
        key_filename=TEST_SSH_KEY_PATH,
    ) as connection:
        connection.execute = mocker.Mock(side_effect=mock_execute)
        connection.md5(path_with_spaces)
        connection.execute.assert_called_with(f"{md5command} {escaped_path_with_spaces}")

