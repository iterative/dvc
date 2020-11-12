from operator import itemgetter

import pytest
from pygtrie import Trie

from dvc.dir_info import DirInfo, _merge
from dvc.hash_info import HashInfo
from dvc.path_info import PosixPathInfo, WindowsPathInfo


@pytest.mark.parametrize(
    "lst, trie_dict",
    [
        ([], {}),
        (
            [
                {"md5": "def", "relpath": "zzz"},
                {"md5": "123", "relpath": "foo"},
                {"md5": "abc", "relpath": "aaa"},
                {"md5": "456", "relpath": "bar"},
            ],
            {
                ("zzz",): HashInfo("md5", "def"),
                ("foo",): HashInfo("md5", "123"),
                ("bar",): HashInfo("md5", "456"),
                ("aaa",): HashInfo("md5", "abc"),
            },
        ),
        (
            [
                {"md5": "123", "relpath": "dir/b"},
                {"md5": "456", "relpath": "dir/z"},
                {"md5": "789", "relpath": "dir/a"},
                {"md5": "abc", "relpath": "b"},
                {"md5": "def", "relpath": "a"},
                {"md5": "ghi", "relpath": "z"},
                {"md5": "jkl", "relpath": "dir/subdir/b"},
                {"md5": "mno", "relpath": "dir/subdir/z"},
                {"md5": "pqr", "relpath": "dir/subdir/a"},
            ],
            {
                ("dir", "b"): HashInfo("md5", "123"),
                ("dir", "z"): HashInfo("md5", "456"),
                ("dir", "a"): HashInfo("md5", "789"),
                ("b",): HashInfo("md5", "abc"),
                ("a",): HashInfo("md5", "def"),
                ("z",): HashInfo("md5", "ghi"),
                ("dir", "subdir", "b"): HashInfo("md5", "jkl"),
                ("dir", "subdir", "z"): HashInfo("md5", "mno"),
                ("dir", "subdir", "a"): HashInfo("md5", "pqr"),
            },
        ),
    ],
)
def test_list(lst, trie_dict):
    dir_info = DirInfo.from_list(lst)
    assert dir_info.trie == Trie(trie_dict)
    assert dir_info.to_list() == sorted(lst, key=itemgetter("relpath"))


@pytest.mark.parametrize(
    "trie_dict, size",
    [
        ({}, 0),
        (
            {
                ("a",): HashInfo("md5", "abc", size=1),
                ("b",): HashInfo("md5", "def", size=2),
                ("c",): HashInfo("md5", "ghi", size=3),
                ("dir", "foo"): HashInfo("md5", "jkl", size=4),
                ("dir", "bar"): HashInfo("md5", "mno", size=5),
                ("dir", "baz"): HashInfo("md5", "pqr", size=6),
            },
            21,
        ),
        (
            {
                ("a",): HashInfo("md5", "abc", size=1),
                ("b",): HashInfo("md5", "def", size=None),
            },
            None,
        ),
    ],
)
def test_size(trie_dict, size):
    dir_info = DirInfo()
    dir_info.trie = Trie(trie_dict)
    assert dir_info.size == size


@pytest.mark.parametrize(
    "trie_dict, nfiles",
    [
        ({}, 0),
        (
            {
                ("a",): HashInfo("md5", "abc", size=1),
                ("b",): HashInfo("md5", "def", size=2),
                ("c",): HashInfo("md5", "ghi", size=3),
                ("dir", "foo"): HashInfo("md5", "jkl", size=4),
                ("dir", "bar"): HashInfo("md5", "mno", size=5),
                ("dir", "baz"): HashInfo("md5", "pqr", size=6),
            },
            6,
        ),
        (
            {
                ("a",): HashInfo("md5", "abc", size=1),
                ("b",): HashInfo("md5", "def", size=None),
            },
            2,
        ),
    ],
)
def test_nfiles(trie_dict, nfiles):
    dir_info = DirInfo()
    dir_info.trie = Trie(trie_dict)
    assert dir_info.nfiles == nfiles


@pytest.mark.parametrize(
    "trie_dict, items",
    [
        ({}, []),
        (
            {
                ("a",): HashInfo("md5", "abc"),
                ("b",): HashInfo("md5", "def"),
                ("c",): HashInfo("md5", "ghi"),
                ("dir", "foo"): HashInfo("md5", "jkl"),
                ("dir", "bar"): HashInfo("md5", "mno"),
                ("dir", "baz"): HashInfo("md5", "pqr"),
                ("dir", "subdir", "1"): HashInfo("md5", "stu"),
                ("dir", "subdir", "2"): HashInfo("md5", "vwx"),
                ("dir", "subdir", "3"): HashInfo("md5", "yz"),
            },
            [
                ("a", HashInfo("md5", "abc")),
                ("b", HashInfo("md5", "def")),
                ("c", HashInfo("md5", "ghi")),
                ("dir/foo", HashInfo("md5", "jkl")),
                ("dir/bar", HashInfo("md5", "mno")),
                ("dir/baz", HashInfo("md5", "pqr")),
                ("dir/subdir/1", HashInfo("md5", "stu")),
                ("dir/subdir/2", HashInfo("md5", "vwx")),
                ("dir/subdir/3", HashInfo("md5", "yz")),
            ],
        ),
    ],
)
def test_items(trie_dict, items):
    dir_info = DirInfo()
    dir_info.trie = Trie(trie_dict)
    assert list(dir_info.items()) == items


@pytest.mark.parametrize(
    "path_info, trie_dict, items",
    [
        (PosixPathInfo(), {}, []),
        (WindowsPathInfo(), {}, []),
        (
            PosixPathInfo("/some/path"),
            {
                ("a",): HashInfo("md5", "abc"),
                ("dir", "foo"): HashInfo("md5", "jkl"),
                ("dir", "sub", "1"): HashInfo("md5", "stu"),
            },
            [
                (PosixPathInfo("/some/path/a"), HashInfo("md5", "abc")),
                (PosixPathInfo("/some/path/dir/foo"), HashInfo("md5", "jkl")),
                (
                    PosixPathInfo("/some/path/dir/sub/1"),
                    HashInfo("md5", "stu"),
                ),
            ],
        ),
        (
            WindowsPathInfo("C:\\some\\path"),
            {
                ("a",): HashInfo("md5", "abc"),
                ("dir", "foo"): HashInfo("md5", "jkl"),
                ("dir", "sub", "1"): HashInfo("md5", "stu"),
            },
            [
                (WindowsPathInfo("C:\\some\\path\\a"), HashInfo("md5", "abc")),
                (
                    WindowsPathInfo("C:\\some\\path\\dir\\foo"),
                    HashInfo("md5", "jkl"),
                ),
                (
                    WindowsPathInfo("C:\\some\\path\\dir\\sub\\1"),
                    HashInfo("md5", "stu"),
                ),
            ],
        ),
    ],
)
def test_items_with_path(path_info, trie_dict, items):
    dir_info = DirInfo()
    dir_info.trie = Trie(trie_dict)
    assert list(dir_info.items(path_info)) == items


@pytest.mark.parametrize(
    "ancestor_dict, our_dict, their_dict, merged_dict",
    [
        ({}, {}, {}, {}),
        (
            {("foo",): HashInfo("md5", "123")},
            {
                ("foo",): HashInfo("md5", "123"),
                ("bar",): HashInfo("md5", "345"),
            },
            {
                ("foo",): HashInfo("md5", "123"),
                ("baz",): HashInfo("md5", "678"),
            },
            {
                ("foo",): HashInfo("md5", "123"),
                ("bar",): HashInfo("md5", "345"),
                ("baz",): HashInfo("md5", "678"),
            },
        ),
        (
            {
                ("common",): HashInfo("md5", "123"),
                ("subdir", "foo"): HashInfo("md5", "345"),
            },
            {
                ("common",): HashInfo("md5", "123"),
                ("subdir", "foo"): HashInfo("md5", "345"),
                ("subdir", "bar"): HashInfo("md5", "678"),
            },
            {
                ("common",): HashInfo("md5", "123"),
                ("subdir", "foo"): HashInfo("md5", "345"),
                ("subdir", "baz"): HashInfo("md5", "91011"),
            },
            {
                ("common",): HashInfo("md5", "123"),
                ("subdir", "foo"): HashInfo("md5", "345"),
                ("subdir", "bar"): HashInfo("md5", "678"),
                ("subdir", "baz"): HashInfo("md5", "91011"),
            },
        ),
        (
            {},
            {("foo",): HashInfo("md5", "123")},
            {("bar",): HashInfo("md5", "456")},
            {
                ("foo",): HashInfo("md5", "123"),
                ("bar",): HashInfo("md5", "456"),
            },
        ),
        (
            {},
            {},
            {("bar",): HashInfo("md5", "123")},
            {("bar",): HashInfo("md5", "123")},
        ),
        (
            {},
            {("bar",): HashInfo("md5", "123")},
            {},
            {("bar",): HashInfo("md5", "123")},
        ),
    ],
)
def test_merge(ancestor_dict, our_dict, their_dict, merged_dict):
    actual = _merge(Trie(ancestor_dict), Trie(our_dict), Trie(their_dict))
    expected = Trie(merged_dict)
    assert actual == expected
