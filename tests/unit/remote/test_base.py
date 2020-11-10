import math

import mock
import pytest

from dvc.cache.base import CloudCache
from dvc.path_info import PathInfo
from dvc.tree.base import BaseTree, RemoteCmdError, RemoteMissingDepsError


class _CallableOrNone:
    """Helper for testing if object is callable() or None."""

    def __eq__(self, other):
        return other is None or callable(other)


CallableOrNone = _CallableOrNone()


def test_missing_deps(dvc):
    requires = {"missing": "missing"}
    with mock.patch.object(BaseTree, "REQUIRES", requires):
        with pytest.raises(RemoteMissingDepsError):
            BaseTree(dvc, {})


def test_cmd_error(dvc):
    config = {}

    cmd = "sed 'hello'"
    ret = "1"
    err = "sed: expression #1, char 2: extra characters after command"

    with mock.patch.object(
        BaseTree, "remove", side_effect=RemoteCmdError("base", cmd, ret, err),
    ):
        with pytest.raises(RemoteCmdError):
            BaseTree(dvc, config).remove("file")


@mock.patch.object(CloudCache, "list_hashes_traverse")
@mock.patch.object(CloudCache, "list_hashes_exists")
def test_hashes_exist(object_exists, traverse, dvc):
    cache = CloudCache(BaseTree(dvc, {}))

    # remote does not support traverse
    cache.tree.CAN_TRAVERSE = False
    with mock.patch.object(
        cache, "list_hashes", return_value=list(range(256))
    ):
        hashes = set(range(1000))
        cache.hashes_exist(hashes)
        object_exists.assert_called_with(hashes, None, None)
        traverse.assert_not_called()

    cache.tree.CAN_TRAVERSE = True

    # large remote, small local
    object_exists.reset_mock()
    traverse.reset_mock()
    with mock.patch.object(
        cache, "list_hashes", return_value=list(range(256))
    ):
        hashes = list(range(1000))
        cache.hashes_exist(hashes)
        # verify that _cache_paths_with_max() short circuits
        # before returning all 256 remote hashes
        max_hashes = math.ceil(
            cache._max_estimation_size(hashes)
            / pow(16, cache.tree.TRAVERSE_PREFIX_LEN)
        )
        assert max_hashes < 256
        object_exists.assert_called_with(
            frozenset(range(max_hashes, 1000)), None, None
        )
        traverse.assert_not_called()

    # large remote, large local
    object_exists.reset_mock()
    traverse.reset_mock()
    cache.tree.JOBS = 16
    with mock.patch.object(
        cache, "list_hashes", return_value=list(range(256))
    ):
        hashes = list(range(1000000))
        cache.hashes_exist(hashes)
        object_exists.assert_not_called()
        traverse.assert_called_with(
            256 * pow(16, cache.tree.TRAVERSE_PREFIX_LEN),
            set(range(256)),
            None,
            None,
        )


@mock.patch.object(
    CloudCache, "list_hashes", return_value=[],
)
@mock.patch.object(
    CloudCache, "_path_to_hash", side_effect=lambda x: x,
)
def test_list_hashes_traverse(_path_to_hash, list_hashes, dvc):
    cache = CloudCache(BaseTree(dvc, {}))
    cache.tree.path_info = PathInfo("foo")

    # parallel traverse
    size = 256 / cache.tree.JOBS * cache.tree.LIST_OBJECT_PAGE_SIZE
    list(cache.list_hashes_traverse(size, {0}))
    for i in range(1, 16):
        list_hashes.assert_any_call(
            prefix=f"{i:03x}", progress_callback=CallableOrNone
        )
    for i in range(1, 256):
        list_hashes.assert_any_call(
            prefix=f"{i:02x}", progress_callback=CallableOrNone
        )

    # default traverse (small remote)
    size -= 1
    list_hashes.reset_mock()
    list(cache.list_hashes_traverse(size - 1, {0}))
    list_hashes.assert_called_with(
        prefix=None, progress_callback=CallableOrNone
    )


def test_list_hashes(dvc):
    cache = CloudCache(BaseTree(dvc, {}))
    cache.tree.path_info = PathInfo("foo")

    with mock.patch.object(
        cache, "_list_paths", return_value=["12/3456", "bar"]
    ):
        hashes = list(cache.list_hashes())
        assert hashes == ["123456"]


def test_list_paths(dvc):
    cache = CloudCache(BaseTree(dvc, {}))
    cache.tree.path_info = PathInfo("foo")

    with mock.patch.object(
        cache.tree, "walk_files", return_value=[]
    ) as walk_mock:
        for _ in cache._list_paths():
            pass
        walk_mock.assert_called_with(cache.tree.path_info, prefix=False)

        for _ in cache._list_paths(prefix="000"):
            pass
        walk_mock.assert_called_with(
            cache.tree.path_info / "00" / "0", prefix=True
        )


@pytest.mark.parametrize(
    "hash_, result",
    [(None, False), ("", False), ("3456.dir", True), ("3456", False)],
)
def test_is_dir_hash(hash_, result):
    assert BaseTree.is_dir_hash(hash_) == result
