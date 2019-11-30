import getpass
import os
import sys
from unittest import TestCase

import pytest
from mock import mock_open
from mock import patch

from dvc.remote.ssh import RemoteSSH
from dvc.system import System
from tests.remotes import get_ssh_url_mocked


class TestRemoteSSH(TestCase):
    def test_url(self):
        user = "test"
        host = "123.45.67.89"
        port = 1234
        path = "/path/to/dir"

        # URL ssh://[user@]host.xz[:port]/path
        url = "ssh://{}@{}:{}{}".format(user, host, port, path)
        config = {"url": url}

        remote = RemoteSSH(None, config)
        self.assertEqual(remote.path_info, url)

        # SCP-like URL ssh://[user@]host.xz:/absolute/path
        url = "ssh://{}@{}:{}".format(user, host, path)
        config = {"url": url}

        remote = RemoteSSH(None, config)
        self.assertEqual(remote.path_info, url)

    def test_no_path(self):
        config = {"url": "ssh://127.0.0.1"}
        remote = RemoteSSH(None, config)
        self.assertEqual(remote.path_info.path, "")


mock_ssh_config = """
Host example.com
   User ubuntu
   HostName 1.2.3.4
   Port 1234
   IdentityFile ~/.ssh/not_default.key
"""

if sys.version_info.major == 3:
    builtin_module_name = "builtins"
else:
    builtin_module_name = "__builtin__"


@pytest.mark.parametrize(
    "config,expected_host",
    [
        ({"url": "ssh://example.com"}, "1.2.3.4"),
        ({"url": "ssh://not_in_ssh_config.com"}, "not_in_ssh_config.com"),
    ],
)
@patch("os.path.exists", return_value=True)
@patch(
    "{}.open".format(builtin_module_name),
    new_callable=mock_open,
    read_data=mock_ssh_config,
)
def test_ssh_host_override_from_config(
    mock_file, mock_exists, config, expected_host
):
    remote = RemoteSSH(None, config)

    mock_exists.assert_called_with(RemoteSSH.ssh_config_filename())
    mock_file.assert_called_with(RemoteSSH.ssh_config_filename())
    assert remote.path_info.host == expected_host


@pytest.mark.parametrize(
    "config,expected_user",
    [
        ({"url": "ssh://test1@example.com"}, "test1"),
        ({"url": "ssh://example.com", "user": "test2"}, "test2"),
        ({"url": "ssh://example.com"}, "ubuntu"),
        ({"url": "ssh://test1@not_in_ssh_config.com"}, "test1"),
        (
            {"url": "ssh://test1@not_in_ssh_config.com", "user": "test2"},
            "test2",
        ),
        ({"url": "ssh://not_in_ssh_config.com"}, getpass.getuser()),
    ],
)
@patch("os.path.exists", return_value=True)
@patch(
    "{}.open".format(builtin_module_name),
    new_callable=mock_open,
    read_data=mock_ssh_config,
)
def test_ssh_user(mock_file, mock_exists, config, expected_user):
    remote = RemoteSSH(None, config)

    mock_exists.assert_called_with(RemoteSSH.ssh_config_filename())
    mock_file.assert_called_with(RemoteSSH.ssh_config_filename())
    assert remote.path_info.user == expected_user


@pytest.mark.parametrize(
    "config,expected_port",
    [
        ({"url": "ssh://example.com:2222"}, 2222),
        ({"url": "ssh://example.com"}, 1234),
        ({"url": "ssh://example.com", "port": 4321}, 4321),
        ({"url": "ssh://not_in_ssh_config.com"}, RemoteSSH.DEFAULT_PORT),
        ({"url": "ssh://not_in_ssh_config.com:2222"}, 2222),
        ({"url": "ssh://not_in_ssh_config.com:2222", "port": 4321}, 4321),
    ],
)
@patch("os.path.exists", return_value=True)
@patch(
    "{}.open".format(builtin_module_name),
    new_callable=mock_open,
    read_data=mock_ssh_config,
)
def test_ssh_port(mock_file, mock_exists, config, expected_port):
    remote = RemoteSSH(None, config)

    mock_exists.assert_called_with(RemoteSSH.ssh_config_filename())
    mock_file.assert_called_with(RemoteSSH.ssh_config_filename())
    assert remote.path_info.port == expected_port


@pytest.mark.parametrize(
    "config,expected_keyfile",
    [
        (
            {"url": "ssh://example.com", "keyfile": "dvc_config.key"},
            "dvc_config.key",
        ),
        (
            {"url": "ssh://example.com"},
            os.path.expanduser("~/.ssh/not_default.key"),
        ),
        (
            {
                "url": "ssh://not_in_ssh_config.com",
                "keyfile": "dvc_config.key",
            },
            "dvc_config.key",
        ),
        ({"url": "ssh://not_in_ssh_config.com"}, None),
    ],
)
@patch("os.path.exists", return_value=True)
@patch(
    "{}.open".format(builtin_module_name),
    new_callable=mock_open,
    read_data=mock_ssh_config,
)
def test_ssh_keyfile(mock_file, mock_exists, config, expected_keyfile):
    remote = RemoteSSH(None, config)

    mock_exists.assert_called_with(RemoteSSH.ssh_config_filename())
    mock_file.assert_called_with(RemoteSSH.ssh_config_filename())
    assert remote.keyfile == expected_keyfile


@pytest.mark.parametrize(
    "config,expected_gss_auth",
    [
        ({"url": "ssh://example.com", "gss_auth": True}, True),
        ({"url": "ssh://example.com", "gss_auth": False}, False),
        ({"url": "ssh://not_in_ssh_config.com"}, False),
    ],
)
@patch("os.path.exists", return_value=True)
@patch(
    "{}.open".format(builtin_module_name),
    new_callable=mock_open,
    read_data=mock_ssh_config,
)
def test_ssh_gss_auth(mock_file, mock_exists, config, expected_gss_auth):
    remote = RemoteSSH(None, config)

    mock_exists.assert_called_with(RemoteSSH.ssh_config_filename())
    mock_file.assert_called_with(RemoteSSH.ssh_config_filename())
    assert remote.gss_auth == expected_gss_auth


def test_hardlink_optimization(repo_dir, ssh_server):
    port = ssh_server.test_creds["port"]
    user = ssh_server.test_creds["username"]

    config = {
        "url": get_ssh_url_mocked(user, port),
        "port": port,
        "user": user,
        "keyfile": ssh_server.test_creds["key_filename"],
    }
    remote = RemoteSSH(None, config)

    from_info = remote.path_info / "empty"
    to_info = remote.path_info / "link"

    with remote.open(from_info, "wb"):
        pass

    if os.name == "nt":
        link_path = "c:" + to_info.path.replace("/", "\\")
    else:
        link_path = to_info.path

    remote.hardlink(from_info, to_info)
    assert not System.is_hardlink(link_path)
