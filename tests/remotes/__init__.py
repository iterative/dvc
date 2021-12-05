from dvc_ssh.tests.fixtures import (  # noqa, pylint: disable=unused-import
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
