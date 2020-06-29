import getpass
import os
from subprocess import CalledProcessError, check_output

import pytest
from funcy import cached_property

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


class SSHMocked(Base):
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

    def __init__(self, server):
        self.server = server

    @cached_property
    def url(self):
        return self.get_url(TEST_SSH_USER, self.server.port)

    @cached_property
    def config(self):
        return {
            "url": self.url,
            "keyfile": TEST_SSH_KEY_PATH,
        }


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

    return SSHMocked(ssh_server)


@pytest.fixture
def ssh_remote(tmp_dir, dvc, ssh):
    tmp_dir.add_remote(config=ssh.config)
    yield ssh
