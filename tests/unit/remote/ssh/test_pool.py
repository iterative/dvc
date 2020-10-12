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
