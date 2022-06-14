import math
import posixpath
from unittest import mock

from dvc.fs import FileSystem
from dvc_data.hashfile.db import ObjectDB


class _CallableOrNone:
    """Helper for testing if object is callable() or None."""

    def __eq__(self, other):
        return other is None or callable(other)


CallableOrNone = _CallableOrNone()


@mock.patch.object(ObjectDB, "_list_oids_traverse")
@mock.patch.object(ObjectDB, "list_oids_exists")
def test_oids_exist(object_exists, traverse, dvc):
    odb = ObjectDB(FileSystem(), None)

    # remote does not support traverse
    odb.fs.CAN_TRAVERSE = False
    with mock.patch.object(odb, "_list_oids", return_value=list(range(256))):
        oids = set(range(1000))
        odb.oids_exist(oids)
        object_exists.assert_called_with(oids, None)
        traverse.assert_not_called()

    odb.fs.CAN_TRAVERSE = True

    # large remote, small local
    object_exists.reset_mock()
    traverse.reset_mock()
    with mock.patch.object(odb, "_list_oids", return_value=list(range(2048))):
        oids = list(range(1000))
        odb.oids_exist(oids)
        # verify that _odb_paths_with_max() short circuits
        # before returning all 2048 remote oids
        max_oids = math.ceil(
            odb._max_estimation_size(oids)
            / pow(16, odb.fs.TRAVERSE_PREFIX_LEN)
        )
        assert max_oids < 2048
        object_exists.assert_called_with(
            frozenset(range(max_oids, 1000)), None
        )
        traverse.assert_not_called()

    # large remote, large local
    object_exists.reset_mock()
    traverse.reset_mock()
    odb.fs._JOBS = 16
    with mock.patch.object(odb, "_list_oids", return_value=list(range(256))):
        oids = list(range(1000000))
        odb.oids_exist(oids)
        object_exists.assert_not_called()
        traverse.assert_called_with(
            256 * pow(16, odb.fs.TRAVERSE_PREFIX_LEN),
            set(range(256)),
            jobs=None,
        )


@mock.patch.object(ObjectDB, "_list_oids", return_value=[])
@mock.patch.object(ObjectDB, "path_to_oid", side_effect=lambda x: x)
def test_list_oids_traverse(
    path_to_oid, list_oids, dvc  # pylint: disable=unused-argument
):
    odb = ObjectDB(FileSystem(), None)
    odb.fs_path = "foo"

    # parallel traverse
    size = 256 / odb.fs._JOBS * odb.fs.LIST_OBJECT_PAGE_SIZE
    list(odb._list_oids_traverse(size, {0}))
    for i in range(1, 16):
        list_oids.assert_any_call(f"{i:0{odb.fs.TRAVERSE_PREFIX_LEN}x}")
    for i in range(1, 256):
        list_oids.assert_any_call(f"{i:02x}")

    # default traverse (small remote)
    size -= 1
    list_oids.reset_mock()
    list(odb._list_oids_traverse(size - 1, {0}))
    list_oids.assert_called_with(None)


def test_list_oids(dvc):
    odb = ObjectDB(FileSystem(), None)
    odb.fs_path = "foo"

    with mock.patch.object(
        odb, "_list_paths", return_value=["12/3456", "bar"]
    ):
        oids = list(odb._list_oids())
        assert oids == ["123456"]


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
