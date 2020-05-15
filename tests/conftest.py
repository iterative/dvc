import os

import mockssh
import pytest

from dvc.remote.ssh.connection import SSHConnection
from tests.utils.httpd import PushRequestHandler, StaticFileServer

from .dir_helpers import *  # noqa

# Prevent updater and analytics from running their processes
os.environ["DVC_TEST"] = "true"
# Ensure progress output even when not outputting to raw sys.stderr console
os.environ["DVC_IGNORE_ISATTY"] = "true"


@pytest.fixture(autouse=True)
def reset_loglevel(request, caplog):
    """
    Use it to ensure log level at the start of each test
    regardless of dvc.logger.setup(), Repo configs or whatever.
    """
    level = request.config.getoption("--log-level")
    if level:
        with caplog.at_level(level.upper(), logger="dvc"):
            yield
    else:
        yield


here = os.path.abspath(os.path.dirname(__file__))

user = "user"
key_path = os.path.join(here, f"{user}.key")


@pytest.fixture
def ssh_server():
    users = {user: key_path}
    with mockssh.Server(users) as s:
        s.test_creds = {
            "host": s.host,
            "port": s.port,
            "username": user,
            "key_filename": key_path,
        }
        yield s


@pytest.fixture
def ssh(ssh_server):
    yield SSHConnection(**ssh_server.test_creds)


@pytest.fixture(scope="session", autouse=True)
def _close_pools():
    from dvc.remote.pool import close_pools

    yield
    close_pools()


@pytest.fixture
def http_server(tmp_dir):
    with StaticFileServer(handler_class=PushRequestHandler) as httpd:
        yield httpd
