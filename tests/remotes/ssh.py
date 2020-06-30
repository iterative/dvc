import getpass
import locale
import os
from contextlib import contextmanager
from subprocess import CalledProcessError, check_output

import pytest
from funcy import cached_property

from dvc.path_info import URLInfo
from dvc.utils import env2bool

from .base import Base
from .local import Local

TEST_SSH_USER = "user"
TEST_SSH_KEY_PATH = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), f"{TEST_SSH_USER}.key"
)


class SSH:
    @staticmethod
    def should_test():
        do_test = env2bool("DVC_TEST_SSH", undefined=None)
        if do_test is not None:
            return do_test

        # FIXME: enable on windows
        if os.name == "nt":
            return False

        try:
            check_output(["ssh", "-o", "BatchMode=yes", "127.0.0.1", "ls"])
        except (CalledProcessError, OSError):
            return False

        return True

    @staticmethod
    def get_url():
        return "ssh://{}@127.0.0.1:22{}".format(
            getpass.getuser(), Local.get_storagepath()
        )


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
            # In our case our ssh server is running with `c:/` as a root,
            # and our URL format requires absolute paths, so the
            # resulting path would look like `/some/path`.
            #
            # [1]https://tools.ietf.org/html/draft-ietf-secsh-filexfer-13#section-6
            drive, path = os.path.splitdrive(path)
            assert drive.lower() == "c:"
            path = path.replace("\\", "/")
        url = f"ssh://{user}@127.0.0.1:{port}{path}"
        return url

    @cached_property
    def config(self):
        return {
            "url": self.url,
            "keyfile": TEST_SSH_KEY_PATH,
        }

    @contextmanager
    def _ssh(self):
        from dvc.remote.ssh.connection import SSHConnection

        conn = SSHConnection(
            host=self.host,
            port=self.port,
            username=TEST_SSH_USER,
            key_filename=TEST_SSH_KEY_PATH,
        )
        try:
            yield conn
        finally:
            conn.close()

    def is_file(self):
        with self._ssh() as _ssh:
            return _ssh.isfile(self.path)

    def is_dir(self):
        with self._ssh() as _ssh:
            return _ssh.isdir(self.path)

    def exists(self):
        with self._ssh() as _ssh:
            return _ssh.exists(self.path)

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents
        assert not exist_ok

        with self._ssh() as _ssh:
            _ssh.makedirs(self.path)

    def write_bytes(self, contents):
        assert isinstance(contents, bytes)
        with self._ssh() as _ssh:
            with _ssh.open(self.path, "w+") as fobj:
                # NOTE: accepts both str and bytes
                fobj.write(contents)

    def write_text(self, contents, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        self.write_bytes(contents.encode(encoding))

    def read_bytes(self):
        with self._ssh() as _ssh:
            # NOTE: sftp always reads in binary format
            with _ssh.open(self.path, "r") as fobj:
                return fobj.read()

    def read_text(self, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        return self.read_bytes().decode(encoding)


@pytest.fixture
def ssh_server():
    import mockssh

    users = {TEST_SSH_USER: TEST_SSH_KEY_PATH}
    with mockssh.Server(users) as s:
        yield s


@pytest.fixture
def ssh_connection(ssh_server):
    from dvc.remote.ssh.connection import SSHConnection

    yield SSHConnection(
        host=ssh_server.host,
        port=ssh_server.port,
        username=TEST_SSH_USER,
        key_filename=TEST_SSH_KEY_PATH,
    )


@pytest.fixture
def ssh(ssh_server, monkeypatch):
    from dvc.remote.ssh import SSHRemoteTree

    # NOTE: see http://github.com/iterative/dvc/pull/3501
    monkeypatch.setattr(SSHRemoteTree, "CAN_TRAVERSE", False)

    return SSHMocked(SSHMocked.get_url(TEST_SSH_USER, ssh_server.port))


@pytest.fixture
def ssh_remote(tmp_dir, dvc, ssh):
    tmp_dir.add_remote(config=ssh.config)
    yield ssh
