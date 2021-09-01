import locale
import os
import uuid

import pytest
from funcy import cached_property

from dvc.path_info import URLInfo

from .base import Base

TEST_SSH_USER = "user"
TEST_SSH_KEY_PATH = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), f"{TEST_SSH_USER}.key"
)


class SSH(Base, URLInfo):
    @staticmethod
    def get_url(host, port):  # pylint: disable=arguments-differ
        return f"ssh://{host}:{port}/tmp/data/{uuid.uuid4()}"

    @cached_property
    def config(self):
        return {
            "url": self.url,
            "user": TEST_SSH_USER,
            "keyfile": TEST_SSH_KEY_PATH,
        }

    @cached_property
    def _ssh(self):
        from sshfs import SSHFileSystem

        return SSHFileSystem(
            host=self.host,
            port=self.port,
            username=TEST_SSH_USER,
            client_keys=[TEST_SSH_KEY_PATH],
        )

    def is_file(self):
        return self._ssh.isfile(self.path)

    def is_dir(self):
        return self._ssh.isdir(self.path)

    def exists(self):
        return self._ssh.exists(self.path)

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents

        self._ssh.makedirs(self.path, exist_ok=exist_ok)

    def write_bytes(self, contents):
        assert isinstance(contents, bytes)
        with self._ssh.open(self.path, "wb") as fobj:
            fobj.write(contents)

    def read_bytes(self):
        with self._ssh.open(self.path, "rb") as fobj:
            return fobj.read()

    def read_text(self, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        return self.read_bytes().decode(encoding)


@pytest.fixture(scope="session")
def ssh_server(test_config, docker_compose, docker_services):
    import asyncssh
    from sshfs import SSHFileSystem

    test_config.requires("ssh")
    conn_info = {
        "host": "127.0.0.1",
        "port": docker_services.port_for("openssh-server", 2222),
    }

    def get_fs():
        return SSHFileSystem(
            **conn_info,
            username=TEST_SSH_USER,
            client_keys=[TEST_SSH_KEY_PATH],
        )

    def _check():
        try:
            get_fs().exists("/")
        except asyncssh.Error:
            return False
        else:
            return True

    docker_services.wait_until_responsive(timeout=30.0, pause=1, check=_check)
    return conn_info


@pytest.fixture
def ssh_connection(ssh_server):
    from sshfs import SSHFileSystem

    yield SSHFileSystem(
        host=ssh_server["host"],
        port=ssh_server["port"],
        username=TEST_SSH_USER,
        client_files=[TEST_SSH_KEY_PATH],
    )


@pytest.fixture
def ssh(ssh_server, monkeypatch):
    from dvc.fs.ssh import SSHFileSystem

    # NOTE: see http://github.com/iterative/dvc/pull/3501
    monkeypatch.setattr(SSHFileSystem, "CAN_TRAVERSE", False)

    url = SSH(SSH.get_url(**ssh_server))
    url.mkdir(exist_ok=True, parents=True)
    return url
