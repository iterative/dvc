import pytest

from dvc.tree.pool import get_connection
from dvc.tree.ssh.connection import SSHConnection
from tests.remotes.ssh import TEST_SSH_KEY_PATH, TEST_SSH_USER

SRC_PATH_WITH_SPECIAL_CHARACTERS = "Escape me [' , ']"
ESCAPED_SRC_PATH_WITH_SPECIAL_CHARACTERS = "'Escape me ['\"'\"' , '\"'\"']'"

DEST_PATH_WITH_SPECIAL_CHARACTERS = "Escape me too [' , ']"
ESCAPED_DEST_PATH_WITH_SPECIAL_CHARACTERS = (
    "'Escape me too ['\"'\"' , '\"'\"']'"
)


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


@pytest.mark.parametrize(
    "uname,md5command", [("Linux", "md5sum"), ("Darwin", "md5")]
)
def test_escapes_filepaths_for_md5_calculation(
    ssh_server, uname, md5command, mocker
):
    fake_md5 = "x" * 32

    with get_connection(
        SSHConnection,
        host=ssh_server.host,
        port=ssh_server.port,
        username=TEST_SSH_USER,
        key_filename=TEST_SSH_KEY_PATH,
    ) as connection:
        uname_mock = mocker.PropertyMock(return_value=uname)
        mocker.patch.object(SSHConnection, "uname", new_callable=uname_mock)
        connection.execute = mocker.Mock(return_value=fake_md5)
        connection.md5(SRC_PATH_WITH_SPECIAL_CHARACTERS)
        connection.execute.assert_called_with(
            f"{md5command} {ESCAPED_SRC_PATH_WITH_SPECIAL_CHARACTERS}"
        )


def test_escapes_filepaths_for_copy(ssh_server, mocker):
    with get_connection(
        SSHConnection,
        host=ssh_server.host,
        port=ssh_server.port,
        username=TEST_SSH_USER,
        key_filename=TEST_SSH_KEY_PATH,
    ) as connection:
        connection.execute = mocker.Mock()
        connection.copy(
            SRC_PATH_WITH_SPECIAL_CHARACTERS, DEST_PATH_WITH_SPECIAL_CHARACTERS
        )
        connection.execute.assert_called_with(
            f"cp {ESCAPED_SRC_PATH_WITH_SPECIAL_CHARACTERS} "
            + f"{ESCAPED_DEST_PATH_WITH_SPECIAL_CHARACTERS}"
        )


@pytest.mark.parametrize(
    "uname,cp_command", [("Linux", "cp --reflink"), ("Darwin", "cp -c")]
)
def test_escapes_filepaths_for_reflink(ssh_server, uname, cp_command, mocker):
    with get_connection(
        SSHConnection,
        host=ssh_server.host,
        port=ssh_server.port,
        username=TEST_SSH_USER,
        key_filename=TEST_SSH_KEY_PATH,
    ) as connection:
        uname_mock = mocker.PropertyMock(return_value=uname)
        mocker.patch.object(SSHConnection, "uname", new_callable=uname_mock)
        connection.execute = mocker.Mock()
        connection.reflink(
            SRC_PATH_WITH_SPECIAL_CHARACTERS, DEST_PATH_WITH_SPECIAL_CHARACTERS
        )
        connection.execute.assert_called_with(
            f"{cp_command} "
            + f"{ESCAPED_SRC_PATH_WITH_SPECIAL_CHARACTERS} "
            + f"{ESCAPED_DEST_PATH_WITH_SPECIAL_CHARACTERS}"
        )


def test_escapes_filepaths_for_hardlink(ssh_server, mocker):
    with get_connection(
        SSHConnection,
        host=ssh_server.host,
        port=ssh_server.port,
        username=TEST_SSH_USER,
        key_filename=TEST_SSH_KEY_PATH,
    ) as connection:
        connection.execute = mocker.Mock()
        connection.hardlink(
            SRC_PATH_WITH_SPECIAL_CHARACTERS, DEST_PATH_WITH_SPECIAL_CHARACTERS
        )
        connection.execute.assert_called_with(
            f"ln {ESCAPED_SRC_PATH_WITH_SPECIAL_CHARACTERS} "
            + f"{ESCAPED_DEST_PATH_WITH_SPECIAL_CHARACTERS}"
        )
