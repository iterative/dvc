from operator import itemgetter

import pytest

from dvc.data.meta import Meta
from dvc.data.tree import Tree, _merge
from dvc.hash_info import HashInfo


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
                ("zzz",): (None, HashInfo("md5", "def")),
                ("foo",): (None, HashInfo("md5", "123")),
                ("bar",): (None, HashInfo("md5", "456")),
                ("aaa",): (None, HashInfo("md5", "abc")),
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
                ("dir", "b"): (
                    None,
                    HashInfo("md5", "123"),
                ),
                ("dir", "z"): (
                    None,
                    HashInfo("md5", "456"),
                ),
                ("dir", "a"): (
                    None,
                    HashInfo("md5", "789"),
                ),
                ("b",): (None, HashInfo("md5", "abc")),
                ("a",): (None, HashInfo("md5", "def")),
                ("z",): (None, HashInfo("md5", "ghi")),
                ("dir", "subdir", "b"): (
                    None,
                    HashInfo("md5", "jkl"),
                ),
                ("dir", "subdir", "z"): (
                    None,
                    HashInfo("md5", "mno"),
                ),
                ("dir", "subdir", "a"): (
                    None,
                    HashInfo("md5", "pqr"),
                ),
            },
        ),
    ],
)
def test_list(lst, trie_dict):
    tree = Tree.from_list(lst)
    assert tree._dict == trie_dict
    assert tree.as_list() == sorted(lst, key=itemgetter("relpath"))


@pytest.mark.parametrize(
    "trie_dict, nfiles",
    [
        ({}, 0),
        (
            {
                ("a",): (Meta(size=1), HashInfo("md5", "abc")),
                ("b",): (Meta(size=2), HashInfo("md5", "def")),
                ("c",): (Meta(size=3), HashInfo("md5", "ghi")),
                ("dir", "foo"): (Meta(size=4), HashInfo("md5", "jkl")),
                ("dir", "bar"): (Meta(size=5), HashInfo("md5", "mno")),
                ("dir", "baz"): (Meta(size=6), HashInfo("md5", "pqr")),
            },
            6,
        ),
        (
            {
                ("a",): (Meta(size=1), HashInfo("md5", "abc")),
                ("b",): (Meta(), HashInfo("md5", "def")),
            },
            2,
        ),
    ],
)
def test_nfiles(trie_dict, nfiles):
    tree = Tree(None, None, None)
    tree._dict = trie_dict
    assert len(tree) == nfiles


@pytest.mark.parametrize(
    "trie_dict",
    [
        {},
        {
            ("a",): (None, HashInfo("md5", "abc")),
            ("b",): (None, HashInfo("md5", "def")),
            ("c",): (None, HashInfo("md5", "ghi")),
            ("dir", "foo"): (None, HashInfo("md5", "jkl")),
            ("dir", "bar"): (None, HashInfo("md5", "mno")),
            ("dir", "baz"): (None, HashInfo("md5", "pqr")),
            ("dir", "subdir", "1"): (None, HashInfo("md5", "stu")),
            ("dir", "subdir", "2"): (None, HashInfo("md5", "vwx")),
            ("dir", "subdir", "3"): (None, HashInfo("md5", "yz")),
        },
    ],
)
def test_items(trie_dict):
    tree = Tree(None, None, None)
    tree._dict = trie_dict
    assert list(tree) == [
        (key, value[0], value[1]) for key, value in trie_dict.items()
    ]


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
    actual = _merge(ancestor_dict, our_dict, their_dict)
    assert actual == merged_dict
