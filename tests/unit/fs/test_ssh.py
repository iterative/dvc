# pylint: disable=W0613

import getpass
import os
from unittest.mock import mock_open, patch

import pytest

from dvc.fs import DEFAULT_SSH_PORT, SSHFileSystem


def test_get_kwargs_from_urls():
    user = "test"
    host = "123.45.67.89"
    port = 1234
    path = "/path/to/dir"

    # URL ssh://[user@]host.xz[:port]/path
    url = f"ssh://{user}@{host}:{port}{path}"

    assert SSHFileSystem._get_kwargs_from_urls(url) == {
        "username": user,
        "host": host,
        "port": port,
    }

    # SCP-like URL ssh://[user@]host.xz:/absolute/path
    url = f"ssh://{user}@{host}:{path}"

    assert SSHFileSystem._get_kwargs_from_urls(url) == {
        "username": user,
        "host": host,
    }


def test_init():
    fs = SSHFileSystem(
        user="test", host="12.34.56.78", port="1234", password="xxx"
    )
    assert fs.fs_args["username"] == "test"
    assert fs.fs_args["host"] == "12.34.56.78"
    assert fs.fs_args["port"] == "1234"
    assert fs.fs_args["password"] == fs.fs_args["passphrase"] == "xxx"


def test_ssh_ask_password(mocker):
    mocker.patch(
        "dvc_objects.fs.implementations.ssh.ask_password", return_value="fish"
    )
    fs = SSHFileSystem(user="test", host="2.2.2.2", ask_password=True)
    assert fs.fs_args["password"] == fs.fs_args["passphrase"] == "fish"


mock_ssh_config = """
Host example.com
   User ubuntu
   HostName 1.2.3.4
   Port 1234
   IdentityFile ~/.ssh/not_default.key
"""


@pytest.mark.parametrize(
    "config,expected_host",
    [
        ({"host": "example.com"}, "1.2.3.4"),
        ({"host": "not_in_ssh_config.com"}, "not_in_ssh_config.com"),
    ],
)
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data=mock_ssh_config,
)
def test_ssh_host_override_from_config(mock_file, config, expected_host):
    fs = SSHFileSystem(**config)
    assert fs.fs_args["host"] == expected_host


@pytest.mark.parametrize(
    "config,expected_user",
    [
        ({"host": "example.com", "user": "test1"}, "test1"),
        ({"host": "example.com"}, "ubuntu"),
        ({"host": "not_in_ssh_config.com", "user": "test1"}, "test1"),
        ({"host": "not_in_ssh_config.com"}, getpass.getuser()),
    ],
)
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data=mock_ssh_config,
)
def test_ssh_user(mock_file, config, expected_user):
    fs = SSHFileSystem(**config)
    assert fs.fs_args["username"] == expected_user


@pytest.mark.parametrize(
    "config,expected_port",
    [
        ({"host": "example.com"}, 1234),
        ({"host": "example.com", "port": 4321}, 4321),
        ({"host": "not_in_ssh_config.com"}, DEFAULT_SSH_PORT),
        ({"host": "not_in_ssh_config.com", "port": 2222}, 2222),
    ],
)
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data=mock_ssh_config,
)
def test_ssh_port(mock_file, config, expected_port):
    fs = SSHFileSystem(**config)
    assert fs.fs_args["port"] == expected_port


@pytest.mark.parametrize(
    "config,expected_keyfile",
    [
        (
            {"host": "example.com", "keyfile": "dvc_config.key"},
            ["dvc_config.key"],
        ),
        (
            {"host": "example.com"},
            ["~/.ssh/not_default.key"],
        ),
        (
            {
                "host": "not_in_ssh_config.com",
                "keyfile": "dvc_config.key",
            },
            ["dvc_config.key"],
        ),
        ({"host": "not_in_ssh_config.com"}, None),
    ],
)
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data=mock_ssh_config,
)
def test_ssh_keyfile(mock_file, config, expected_keyfile):
    fs = SSHFileSystem(**config)
    expected_keyfiles = (
        [os.path.expanduser(path) for path in expected_keyfile]
        if expected_keyfile
        else expected_keyfile
    )
    assert fs.fs_args.get("client_keys") == expected_keyfiles


mock_ssh_multi_key_config = """
IdentityFile file_1

Host example.com
    IdentityFile file_2
"""


@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data=mock_ssh_multi_key_config,
)
def test_ssh_multi_identity_files(mock_file):
    fs = SSHFileSystem(host="example.com")
    assert fs.fs_args.get("client_keys") == ["file_1", "file_2"]


@pytest.mark.parametrize(
    "config,expected_gss_auth",
    [
        ({"host": "example.com", "gss_auth": True}, True),
        ({"host": "example.com", "gss_auth": False}, False),
        ({"host": "not_in_ssh_config.com"}, False),
    ],
)
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data=mock_ssh_config,
)
def test_ssh_gss_auth(mock_file, config, expected_gss_auth):
    fs = SSHFileSystem(**config)
    assert fs.fs_args["gss_auth"] == expected_gss_auth
