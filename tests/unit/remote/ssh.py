import getpass

from unittest import TestCase

from dvc.remote.ssh import RemoteSSH


class TestRemoteSSH(TestCase):
    def test_user(self):
        url = 'ssh://127.0.0.1:/path/to/dir'
        config = {'url': url}
        remote = RemoteSSH(None, config)
        self.assertEqual(remote.user, getpass.getuser())

        user = 'test1'
        url = 'ssh://{}@127.0.0.1:/path/to/dir'.format(user)
        config = {'url': url}
        remote = RemoteSSH(None, config)
        self.assertEqual(remote.user, user)

        user = 'test2'
        config['user'] = user
        remote = RemoteSSH(None, config)
        self.assertEqual(remote.user, user)
