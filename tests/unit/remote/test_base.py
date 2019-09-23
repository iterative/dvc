import mock
from unittest import TestCase

from dvc.remote.base import RemoteBASE, RemoteCmdError, RemoteMissingDepsError


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


class TestCacheExists(TestCase):
    def test(self):
        config = {
            "url": "base://example/prefix",
            "connection_string": "1234567",
        }
        remote = RemoteBASE(None, config)

        remote.PARAM_CHECKSUM = "checksum"
        remote.path_info = remote.path_cls(config["url"])
        remote.url = ""
        remote.prefix = ""
        path_info = remote.path_info / "example"
        checksum_info = {remote.PARAM_CHECKSUM: "1234567890"}

        with mock.patch.object(remote, "_checkout") as mock_checkout:
            with mock.patch.object(remote, "_save") as mock_save:
                with mock.patch.object(
                    remote, "changed_cache", return_value=True
                ):
                    remote.save(path_info, checksum_info)
                    mock_save.assert_called_once()
                    mock_checkout.assert_not_called()

        with mock.patch.object(remote, "_checkout") as mock_checkout:
            with mock.patch.object(remote, "_save") as mock_save:
                with mock.patch.object(
                    remote, "changed_cache", return_value=False
                ):
                    remote.save(path_info, checksum_info)
                    mock_save.assert_not_called()
                    mock_checkout.assert_called_once()
