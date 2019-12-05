from unittest import TestCase

import mock

from dvc.remote.base import RemoteBASE
from dvc.remote.base import RemoteCmdError
from dvc.remote.base import RemoteMissingDepsError


class TestRemoteBASE(object):
    REMOTE_CLS = RemoteBASE


class TestMissingDeps(TestCase, TestRemoteBASE):
    def test(self):
        requires = {"missing": "missing"}
        with mock.patch.object(self.REMOTE_CLS, "REQUIRES", requires):
            with self.assertRaises(RemoteMissingDepsError):
                self.REMOTE_CLS(None, {})


class TestCmdError(TestCase, TestRemoteBASE):
    def test(self):
        repo = None
        config = {}

        cmd = "sed 'hello'"
        ret = "1"
        err = "sed: expression #1, char 2: extra characters after command"

        with mock.patch.object(
            self.REMOTE_CLS,
            "remove",
            side_effect=RemoteCmdError("base", cmd, ret, err),
        ):
            with self.assertRaises(RemoteCmdError):
                self.REMOTE_CLS(repo, config).remove("file")
