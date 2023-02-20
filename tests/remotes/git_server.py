import pytest
from dvc_ssh.tests.cloud import SSH, TEST_SSH_KEY_PATH, TEST_SSH_USER


class GitSSH(SSH):
    @staticmethod
    def get_url(host, port):
        return f"ssh://{host}:{port}/tmp/data/git"


@pytest.fixture
def git_server(request, test_config):
    import asyncssh
    from sshfs import SSHFileSystem

    test_config.requires("ssh")
    docker_services = request.getfixturevalue("docker_services")
    conn_info = {
        "host": "127.0.0.1",
        "port": docker_services.port_for("git-server", 2222),
    }

    def get_fs():
        return SSHFileSystem(
            **conn_info,
            username=TEST_SSH_USER,
            client_keys=[TEST_SSH_KEY_PATH],
        )

    def _check():
        try:
            fs = get_fs()
            fs.exists("/")
            fs.execute("git --version")
        except asyncssh.Error:
            return False
        else:
            return True

    docker_services.wait_until_responsive(timeout=30.0, pause=1, check=_check)
    return conn_info


@pytest.fixture
def git_ssh_connection(git_server):
    from sshfs import SSHFileSystem

    return SSHFileSystem(
        host=git_server["host"],
        port=git_server["port"],
        username=TEST_SSH_USER,
        client_keys=[TEST_SSH_KEY_PATH],
    )


@pytest.fixture
def git_ssh(git_server, monkeypatch):
    url = GitSSH(GitSSH.get_url(**git_server))
    url.mkdir(exist_ok=True, parents=True)
    return url
