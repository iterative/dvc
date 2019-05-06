import getpass
import os
import sys

from unittest import TestCase

import mock
from mock import patch
import pytest

from dvc.remote.ssh import RemoteSSH


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

        self.assertEqual(remote.user, user)
        self.assertEqual(remote.host, host)
        self.assertEqual(remote.port, port)
        self.assertEqual(remote.prefix, path)

        # SCP-like URL ssh://[user@]host.xz:/absolute/path
        url = "ssh://{}@{}:{}".format(user, host, path)
        config = {"url": url}

        remote = RemoteSSH(None, config)

        self.assertEqual(remote.user, user)
        self.assertEqual(remote.host, host)
        self.assertEqual(remote.port, remote.DEFAULT_PORT)
        self.assertEqual(remote.prefix, path)

    def test_no_path(self):
        config = {"url": "ssh://127.0.0.1"}
        remote = RemoteSSH(None, config)
        self.assertEqual(remote.prefix, "/")


mock_ssh_config = """
Host example.com
   User ubuntu
   HostName 1.2.3.4
   Port 1234
   IdentityFile ~/.ssh/not_default.key
"""

# compat version for mock library version 2
# mock 2.0.0 can't iterate in mock_open
if mock.version_info[0] < 3:

    def mock_open_compat(*args, **kargs):
        f_open = mock.mock_open(*args, **kargs)
        if sys.version_info.major == 3:
            f_open.return_value.__iter__ = lambda self: self
            f_open.return_value.__next__ = lambda self: self.readline()
        else:
            f_open.return_value.__iter__ = lambda self: iter(self.readline, "")

        return f_open

    mock_open = mock_open_compat
else:
    mock_open = mock.mock_open

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

    mock_exists.assert_called_with(os.path.expanduser("~/.ssh/config"))
    mock_file.assert_called_with(os.path.expanduser("~/.ssh/config"))
    assert remote.host == expected_host


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

    mock_exists.assert_called_with(os.path.expanduser("~/.ssh/config"))
    mock_file.assert_called_with(os.path.expanduser("~/.ssh/config"))
    assert remote.user == expected_user


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

    mock_exists.assert_called_with(os.path.expanduser("~/.ssh/config"))
    mock_file.assert_called_with(os.path.expanduser("~/.ssh/config"))
    assert remote.port == expected_port


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

    mock_exists.assert_called_with(os.path.expanduser("~/.ssh/config"))
    mock_file.assert_called_with(os.path.expanduser("~/.ssh/config"))
    assert remote.keyfile == expected_keyfile
