import math
import mock
import pytest

from dvc.path_info import PathInfo
from dvc.remote.base import BaseRemote
from dvc.remote.base import RemoteCmdError
from dvc.remote.base import RemoteMissingDepsError


class _CallableOrNone(object):
    """Helper for testing if object is callable() or None."""

    def __eq__(self, other):
        return other is None or callable(other)


CallableOrNone = _CallableOrNone()
REMOTE_CLS = BaseRemote


def test_missing_deps(dvc):
    requires = {"missing": "missing"}
    with mock.patch.object(REMOTE_CLS, "REQUIRES", requires):
        with pytest.raises(RemoteMissingDepsError):
            REMOTE_CLS(dvc, {})


def test_cmd_error(dvc):
    config = {}

    cmd = "sed 'hello'"
    ret = "1"
    err = "sed: expression #1, char 2: extra characters after command"

    with mock.patch.object(
        REMOTE_CLS,
        "remove",
        side_effect=RemoteCmdError("base", cmd, ret, err),
    ):
        with pytest.raises(RemoteCmdError):
            REMOTE_CLS(dvc, config).remove("file")


@mock.patch.object(BaseRemote, "_cache_checksums_traverse")
@mock.patch.object(BaseRemote, "_cache_object_exists")
def test_cache_exists(object_exists, traverse, dvc):
    remote = BaseRemote(dvc, {})

    # remote does not support traverse
    remote.CAN_TRAVERSE = False
    with mock.patch.object(
        remote, "cache_checksums", return_value=list(range(256))
    ):
        checksums = set(range(1000))
        remote.cache_exists(checksums)
        object_exists.assert_called_with(checksums, None, None)
        traverse.assert_not_called()

    remote.CAN_TRAVERSE = True

    # large remote, small local
    object_exists.reset_mock()
    traverse.reset_mock()
    with mock.patch.object(
        remote, "cache_checksums", return_value=list(range(256))
    ):
        checksums = list(range(1000))
        remote.cache_exists(checksums)
        # verify that _cache_paths_with_max() short circuits
        # before returning all 256 remote checksums
        max_checksums = math.ceil(
            remote._max_estimation_size(checksums)
            / pow(16, remote.TRAVERSE_PREFIX_LEN)
        )
        assert max_checksums < 256
        object_exists.assert_called_with(
            frozenset(range(max_checksums, 1000)), None, None
        )
        traverse.assert_not_called()

    # large remote, large local
    object_exists.reset_mock()
    traverse.reset_mock()
    remote.JOBS = 16
    with mock.patch.object(
        remote, "cache_checksums", return_value=list(range(256))
    ):
        checksums = list(range(1000000))
        remote.cache_exists(checksums)
        object_exists.assert_not_called()
        traverse.assert_called_with(
            256 * pow(16, remote.TRAVERSE_PREFIX_LEN),
            set(range(256)),
            None,
            None,
        )


@mock.patch.object(
    BaseRemote, "cache_checksums", return_value=[],
)
@mock.patch.object(
    BaseRemote, "path_to_checksum", side_effect=lambda x: x,
)
def test_cache_checksums_traverse(path_to_checksum, cache_checksums, dvc):
    remote = BaseRemote(dvc, {})
    remote.path_info = PathInfo("foo")

    # parallel traverse
    size = 256 / remote.JOBS * remote.LIST_OBJECT_PAGE_SIZE
    list(remote._cache_checksums_traverse(size, {0}))
    for i in range(1, 16):
        cache_checksums.assert_any_call(
            prefix="{:03x}".format(i), progress_callback=CallableOrNone
        )
    for i in range(1, 256):
        cache_checksums.assert_any_call(
            prefix="{:02x}".format(i), progress_callback=CallableOrNone
        )

    # default traverse (small remote)
    size -= 1
    cache_checksums.reset_mock()
    list(remote._cache_checksums_traverse(size - 1, {0}))
    cache_checksums.assert_called_with(
        prefix=None, progress_callback=CallableOrNone
    )


def test_cache_checksums(dvc):
    remote = BaseRemote(dvc, {})
    remote.path_info = PathInfo("foo")

    with mock.patch.object(
        remote, "list_cache_paths", return_value=["12/3456", "bar"]
    ):
        checksums = list(remote.cache_checksums())
        assert checksums == ["123456"]
