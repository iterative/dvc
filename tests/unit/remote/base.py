import mock
from unittest import TestCase

from dvc.remote.base import RemoteBase, RemoteMissingDepsError
from dvc.remote import REMOTES, RemoteLOCAL


class TestMissingDeps(TestCase):
    def test(self):
        remotes = REMOTES + [RemoteLOCAL, RemoteBase]
        for remote_class in remotes:
            REQUIRES = {"foo": None, "bar": None, "mock": mock}
            with mock.patch.object(remote_class, "REQUIRES", REQUIRES):
                with self.assertRaises(RemoteMissingDepsError):
                    remote_class(None, {})
