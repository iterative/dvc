from unittest import TestCase

import mock

from dvc.path_info import PathInfo
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


@mock.patch.object(RemoteBASE, "_cache_exists_traverse")
@mock.patch.object(RemoteBASE, "_cache_object_exists")
@mock.patch.object(
    RemoteBASE, "path_to_checksum", side_effect=lambda x: x,
)
def test_cache_exists(path_to_checksum, object_exists, traverse):
    remote = RemoteBASE(None, {})

    # remote does not support traverse
    remote.CAN_TRAVERSE = False
    with mock.patch.object(
        remote, "list_cache_paths", return_value=list(range(256))
    ):
        checksums = list(range(1000))
        remote.cache_exists(checksums)
        object_exists.assert_called_with(checksums, None, None)
        traverse.assert_not_called()

    remote.CAN_TRAVERSE = True

    # large remote, small local
    object_exists.reset_mock()
    traverse.reset_mock()
    with mock.patch.object(
        remote, "list_cache_paths", return_value=list(range(256))
    ):
        checksums = list(range(1000))
        remote.cache_exists(checksums)
        object_exists.assert_called_with(
            frozenset(range(256, 1000)), None, None
        )
        traverse.assert_not_called()

    # large remote, large local
    object_exists.reset_mock()
    traverse.reset_mock()
    remote.JOBS = 16
    with mock.patch.object(
        remote, "list_cache_paths", return_value=list(range(256))
    ):
        checksums = list(range(1000000))
        remote.cache_exists(checksums)
        object_exists.assert_not_called()
        traverse.assert_called_with(
            frozenset(checksums), set(range(256)), None, None
        )

    # default traverse
    object_exists.reset_mock()
    traverse.reset_mock()
    remote.TRAVERSE_WEIGHT_MULTIPLIER = 1
    with mock.patch.object(remote, "list_cache_paths", return_value=[0]):
        checksums = set(range(1000000))
        remote.cache_exists(checksums)
        traverse.assert_not_called()
        object_exists.assert_not_called()


@mock.patch.object(
    RemoteBASE, "list_cache_paths", return_value=[],
)
@mock.patch.object(
    RemoteBASE, "path_to_checksum", side_effect=lambda x: x,
)
def test_cache_exists_traverse(path_to_checksum, list_cache_paths):
    remote = RemoteBASE(None, {})
    remote.path_info = PathInfo("foo")
    remote._cache_exists_traverse({0}, set())
    for i in range(1, 16):
        list_cache_paths.assert_any_call(prefix="{:03x}".format(i))
    for i in range(1, 256):
        list_cache_paths.assert_any_call(prefix="{:02x}".format(i))
