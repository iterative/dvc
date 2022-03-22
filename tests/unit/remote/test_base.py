import math
import posixpath
from unittest import mock

from dvc.fs.base import FileSystem
from dvc.objects.db import ObjectDB


class _CallableOrNone:
    """Helper for testing if object is callable() or None."""

    def __eq__(self, other):
        return other is None or callable(other)


CallableOrNone = _CallableOrNone()


@mock.patch.object(ObjectDB, "_list_hashes_traverse")
@mock.patch.object(ObjectDB, "list_hashes_exists")
def test_hashes_exist(object_exists, traverse, dvc):
    odb = ObjectDB(FileSystem(), None)

    # remote does not support traverse
    odb.fs.CAN_TRAVERSE = False
    with mock.patch.object(odb, "_list_hashes", return_value=list(range(256))):
        hashes = set(range(1000))
        odb.hashes_exist(hashes)
        object_exists.assert_called_with(hashes, None, None)
        traverse.assert_not_called()

    odb.fs.CAN_TRAVERSE = True

    # large remote, small local
    object_exists.reset_mock()
    traverse.reset_mock()
    with mock.patch.object(odb, "_list_hashes", return_value=list(range(256))):
        hashes = list(range(1000))
        odb.hashes_exist(hashes)
        # verify that _odb_paths_with_max() short circuits
        # before returning all 256 remote hashes
        max_hashes = math.ceil(
            odb._max_estimation_size(hashes)
            / pow(16, odb.fs.TRAVERSE_PREFIX_LEN)
        )
        assert max_hashes < 256
        object_exists.assert_called_with(
            frozenset(range(max_hashes, 1000)), None, None
        )
        traverse.assert_not_called()

    # large remote, large local
    object_exists.reset_mock()
    traverse.reset_mock()
    odb.fs._JOBS = 16
    with mock.patch.object(odb, "_list_hashes", return_value=list(range(256))):
        hashes = list(range(1000000))
        odb.hashes_exist(hashes)
        object_exists.assert_not_called()
        traverse.assert_called_with(
            256 * pow(16, odb.fs.TRAVERSE_PREFIX_LEN),
            set(range(256)),
            jobs=None,
            name=None,
        )


@mock.patch.object(ObjectDB, "_list_hashes", return_value=[])
@mock.patch.object(ObjectDB, "_path_to_hash", side_effect=lambda x: x)
def test_list_hashes_traverse(_path_to_hash, list_hashes, dvc):
    odb = ObjectDB(FileSystem(), None)
    odb.fs_path = "foo"

    # parallel traverse
    size = 256 / odb.fs._JOBS * odb.fs.LIST_OBJECT_PAGE_SIZE
    list(odb._list_hashes_traverse(size, {0}))
    for i in range(1, 16):
        list_hashes.assert_any_call(f"{i:03x}")
    for i in range(1, 256):
        list_hashes.assert_any_call(f"{i:02x}")

    # default traverse (small remote)
    size -= 1
    list_hashes.reset_mock()
    list(odb._list_hashes_traverse(size - 1, {0}))
    list_hashes.assert_called_with(None)


def test_list_hashes(dvc):
    odb = ObjectDB(FileSystem(), None)
    odb.fs_path = "foo"

    with mock.patch.object(
        odb, "_list_paths", return_value=["12/3456", "bar"]
    ):
        hashes = list(odb._list_hashes())
        assert hashes == ["123456"]


def test_list_paths(dvc):
    path = "foo"
    odb = ObjectDB(FileSystem(), path)

    with mock.patch.object(odb.fs, "find", return_value=[]) as walk_mock:
        for _ in odb._list_paths():
            pass
        walk_mock.assert_called_with(path, prefix=False)

        for _ in odb._list_paths(prefix="000"):
            pass
        walk_mock.assert_called_with(
            posixpath.join(path, "00", "0"), prefix=True
        )
