import locale
import os
import subprocess

import mockssh
import pytest
from funcy import cached_property
from mockssh.server import Handler

from dvc.path_info import URLInfo

from .base import Base
from .local import Local

TEST_SSH_USER = "user"
TEST_SSH_KEY_PATH = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), f"{TEST_SSH_USER}.key"
)


# See: https://github.com/carletes/mock-ssh-server/issues/22
class SSHMockHandler(Handler):
    def handle_client(self, channel):
        try:
            command = self.command_queues[channel.chanid].get(block=True)
            self.log.debug("Executing %s", command)
            if isinstance(command, bytes):
                command = command.decode()
            p = subprocess.Popen(
                command,
                shell=True,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = p.communicate()
            channel.sendall(stdout)
            channel.sendall_stderr(stderr)
            channel.send_exit_status(p.returncode)
        except Exception:  # pylint: disable=broad-except
            self.log.error(
                "Error handling client (channel: %s)", channel, exc_info=True
            )
        finally:
            try:
                channel.close()
            except EOFError:
                self.log.debug("Tried to close already closed channel")


class SSHMockServer(mockssh.Server):
    handler_cls = SSHMockHandler


class SSHMocked(Base, URLInfo):
    @staticmethod
    def get_url(user, port):  # pylint: disable=arguments-differ
        path = Local.get_storagepath()
        if os.name == "nt":
            # NOTE: On Windows Local.get_storagepath() will return an
            # ntpath that looks something like `C:\some\path`, which is not
            # compatible with SFTP paths [1], so we need to convert it to
            # a proper posixpath.
            # To do that, we should construct a posixpath that would be
            # relative to the server's root.
            # Our URL format requires absolute paths, so the
            # resulting path would look like `/some/path`.
            #
            # [1]https://tools.ietf.org/html/draft-ietf-secsh-filexfer-13#section-6
            drive, path = os.path.splitdrive(path)

            # Hackish way to make sure SSH server runs on same drive as tests
            # and temporary directories are.
            # Context: https://github.com/iterative/dvc/pull/4660
            test_drive, _ = os.path.splitdrive(os.getcwd())
            if drive.lower() != test_drive.lower():
                raise Exception("Did you forget to use `tmp_dir?`")

            path = path.replace("\\", "/")
        url = f"ssh://{user}@127.0.0.1:{port}{path}"
        return url

    @cached_property
    def config(self):
        return {"url": self.url, "keyfile": TEST_SSH_KEY_PATH}

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


@pytest.fixture
def ssh_server(test_config):
    test_config.requires("ssh")
    users = {TEST_SSH_USER: TEST_SSH_KEY_PATH}
    with SSHMockServer(users) as s:
        yield s


@pytest.fixture
def ssh_connection(ssh_server):
    from sshfs import SSHFileSystem

    yield SSHFileSystem(
        host=ssh_server.host,
        port=ssh_server.port,
        username=TEST_SSH_USER,
        client_files=[TEST_SSH_KEY_PATH],
    )


@pytest.fixture
def ssh(ssh_server, monkeypatch):
    from dvc.fs.ssh import SSHFileSystem

    # NOTE: see http://github.com/iterative/dvc/pull/3501
    monkeypatch.setattr(SSHFileSystem, "CAN_TRAVERSE", False)

    return SSHMocked(SSHMocked.get_url(TEST_SSH_USER, ssh_server.port))
