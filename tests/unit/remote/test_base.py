import math

import mock
import pytest

from dvc.path_info import PathInfo
from dvc.remote.base import (
    BaseRemoteTree,
    Remote,
    RemoteCmdError,
    RemoteMissingDepsError,
)


class _CallableOrNone:
    """Helper for testing if object is callable() or None."""

    def __eq__(self, other):
        return other is None or callable(other)


CallableOrNone = _CallableOrNone()


def test_missing_deps(dvc):
    requires = {"missing": "missing"}
    with mock.patch.object(BaseRemoteTree, "REQUIRES", requires):
        with pytest.raises(RemoteMissingDepsError):
            BaseRemoteTree(dvc, {})


def test_cmd_error(dvc):
    config = {}

    cmd = "sed 'hello'"
    ret = "1"
    err = "sed: expression #1, char 2: extra characters after command"

    with mock.patch.object(
        BaseRemoteTree,
        "remove",
        side_effect=RemoteCmdError("base", cmd, ret, err),
    ):
        with pytest.raises(RemoteCmdError):
            BaseRemoteTree(dvc, config).remove("file")


@mock.patch.object(BaseRemoteTree, "list_checksums_traverse")
@mock.patch.object(BaseRemoteTree, "list_checksums_exists")
def test_checksums_exist(object_exists, traverse, dvc):
    remote = Remote(BaseRemoteTree(dvc, {}))

    # remote does not support traverse
    remote.tree.CAN_TRAVERSE = False
    with mock.patch.object(
        remote.tree, "list_checksums", return_value=list(range(256))
    ):
        checksums = set(range(1000))
        remote.checksums_exist(checksums)
        object_exists.assert_called_with(checksums, None, None)
        traverse.assert_not_called()

    remote.tree.CAN_TRAVERSE = True

    # large remote, small local
    object_exists.reset_mock()
    traverse.reset_mock()
    with mock.patch.object(
        remote.tree, "list_checksums", return_value=list(range(256))
    ):
        checksums = list(range(1000))
        remote.checksums_exist(checksums)
        # verify that _cache_paths_with_max() short circuits
        # before returning all 256 remote checksums
        max_checksums = math.ceil(
            remote.tree._max_estimation_size(checksums)
            / pow(16, remote.tree.TRAVERSE_PREFIX_LEN)
        )
        assert max_checksums < 256
        object_exists.assert_called_with(
            frozenset(range(max_checksums, 1000)), None, None
        )
        traverse.assert_not_called()

    # large remote, large local
    object_exists.reset_mock()
    traverse.reset_mock()
    remote.tree.JOBS = 16
    with mock.patch.object(
        remote.tree, "list_checksums", return_value=list(range(256))
    ):
        checksums = list(range(1000000))
        remote.checksums_exist(checksums)
        object_exists.assert_not_called()
        traverse.assert_called_with(
            256 * pow(16, remote.tree.TRAVERSE_PREFIX_LEN),
            set(range(256)),
            None,
            None,
        )


@mock.patch.object(
    BaseRemoteTree, "list_checksums", return_value=[],
)
@mock.patch.object(
    BaseRemoteTree, "path_to_checksum", side_effect=lambda x: x,
)
def test_list_checksums_traverse(path_to_checksum, list_checksums, dvc):
    tree = BaseRemoteTree(dvc, {})
    tree.path_info = PathInfo("foo")

    # parallel traverse
    size = 256 / tree.JOBS * tree.LIST_OBJECT_PAGE_SIZE
    list(tree.list_checksums_traverse(size, {0}))
    for i in range(1, 16):
        list_checksums.assert_any_call(
            prefix=f"{i:03x}", progress_callback=CallableOrNone
        )
    for i in range(1, 256):
        list_checksums.assert_any_call(
            prefix=f"{i:02x}", progress_callback=CallableOrNone
        )

    # default traverse (small remote)
    size -= 1
    list_checksums.reset_mock()
    list(tree.list_checksums_traverse(size - 1, {0}))
    list_checksums.assert_called_with(
        prefix=None, progress_callback=CallableOrNone
    )


def test_list_checksums(dvc):
    tree = BaseRemoteTree(dvc, {})
    tree.path_info = PathInfo("foo")

    with mock.patch.object(
        tree, "list_paths", return_value=["12/3456", "bar"]
    ):
        checksums = list(tree.list_checksums())
        assert checksums == ["123456"]


@pytest.mark.parametrize(
    "checksum, result",
    [(None, False), ("", False), ("3456.dir", True), ("3456", False)],
)
def test_is_dir_checksum(checksum, result):
    assert BaseRemoteTree.is_dir_checksum(checksum) == result
