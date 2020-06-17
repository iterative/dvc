import getpass
import os
from subprocess import CalledProcessError, check_output

import pytest

from dvc.utils import env2bool

from .base import Base
from .local import Local


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
    def get_url(user, port):
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


@pytest.fixture(scope="session", autouse=True)
def ssh_server():
    import mockssh

    user = "user"
    path = os.path.abspath(os.path.dirname(__file__))
    key_path = os.path.join(path, f"{user}.key")
    users = {user: key_path}
    with mockssh.Server(users) as s:
        yield {
            "host": s.host,
            "port": s.port,
            "username": user,
            "key_filename": key_path,
        }


@pytest.fixture
def ssh_connection(ssh_server):
    from dvc.remote.ssh.connection import SSHConnection

    yield SSHConnection(**ssh_server)


@pytest.fixture
def ssh(ssh_server, monkeypatch):
    from dvc.remote.ssh import SSHRemoteTree

    # NOTE: see http://github.com/iterative/dvc/pull/3501
    monkeypatch.setattr(SSHRemoteTree, "CAN_TRAVERSE", False)

    return {
        "url": SSHMocked.get_url(ssh_server["username"], ssh_server["port"]),
        "keyfile": ssh_server["key_filename"],
    }


@pytest.fixture
def ssh_remote(tmp_dir, dvc, ssh):
    tmp_dir.add_remote(config=ssh)
    yield ssh
