import pytest

from dvc.remote.pool import get_connection
from dvc.remote.ssh.connection import SSHConnection


def test_doesnt_swallow_errors(ssh_server):
    class MyError(Exception):
        pass

    with pytest.raises(MyError), get_connection(
        SSHConnection, **ssh_server.test_creds
    ):
        raise MyError
