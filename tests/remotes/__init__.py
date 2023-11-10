from dvc_ssh.tests.fixtures import (  # noqa: F401
    make_ssh,
    ssh,
    ssh_server,
)

from .git_server import git_server, git_ssh  # noqa: F401

TEST_REMOTE = "upstream"
TEST_CONFIG = {
    "cache": {},
    "core": {"remote": TEST_REMOTE},
    "remote": {TEST_REMOTE: {"url": ""}},
}
