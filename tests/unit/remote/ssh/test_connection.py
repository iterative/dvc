import mockssh
import os
from pytest import yield_fixture

from dvc.remote.ssh.connection import SSHConnection

here = os.path.abspath(os.path.dirname(__file__))

user = "user"
key_path = os.path.join(here, "{0}.key".format(user))


@yield_fixture()
def server():
    users = {user: key_path}
    with mockssh.Server(users) as s:
        yield s


def _client(server):
    client = SSHConnection(
        server.host, username=user, port=server.port, key_filename=key_path
    )
    return client


def test_connection(server):
    client = _client(server)
    ls = client.execute("ls /")
    assert ls


def test_isdir(server):
    client = _client(server)
    path = here
    isdir = client.isdir(path=path)
    assert isdir is True


def test_file_exists(server):
    client = _client(server)
    path = "/path/to/file"
    file_path = client.file_exists(path=path)
    assert file_path is False
