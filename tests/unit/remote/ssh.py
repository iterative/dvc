import getpass

from unittest import TestCase

from dvc.remote.ssh import RemoteSSH


class TestRemoteSSH(TestCase):
    def test_user(self):
        url = "ssh://127.0.0.1:/path/to/dir"
        config = {"url": url}
        remote = RemoteSSH(None, config)
        self.assertEqual(remote.user, getpass.getuser())

        user = "test1"
        url = "ssh://{}@127.0.0.1:/path/to/dir".format(user)
        config = {"url": url}
        remote = RemoteSSH(None, config)
        self.assertEqual(remote.user, user)

        user = "test2"
        config["user"] = user
        remote = RemoteSSH(None, config)
        self.assertEqual(remote.user, user)

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

    def test_port(self):
        url = "ssh://127.0.0.1/path/to/dir"
        config = {"url": url}
        remote = RemoteSSH(None, config)
        self.assertEqual(remote.port, remote.DEFAULT_PORT)

        port = 1234
        url = "ssh://127.0.0.1:{}/path/to/dir".format(port)
        config = {"url": url}
        remote = RemoteSSH(None, config)
        self.assertEqual(remote.port, port)

        port = 4321
        config["port"] = port
        remote = RemoteSSH(None, config)
        self.assertEqual(remote.port, port)
