import pytest

from dvc.remote.ssh.pool import ssh_connection


def test_doesnt_swallow_errors(ssh_server):
    class MyError(Exception):
        pass

    with pytest.raises(MyError), ssh_connection(**ssh_server.test_creds):
        raise MyError
