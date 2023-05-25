import json

import pytest

from dvc.utils.collections import (
    apply_diff,
    merge_dicts,
    remove_missing_keys,
    to_omegaconf,
)
from dvc.utils.serialize import dumps_yaml


class MyDict(dict):
    pass


class MyInt(int):
    pass


def test_apply_diff_is_inplace():
    dest = MyDict()
    dest.attr = 42
    apply_diff({}, dest)

    assert type(dest) is MyDict, "Preserves class"
    assert dest.attr == 42, "Preserves custom attrs"


def test_apply_diff_mapping():
    src = {"a": 1}
    dest = {"b": 2}
    apply_diff(src, dest)
    assert dest == src, "Adds and removes keys"

    src = {"a": 1}
    dest = {"a": MyInt(1)}
    apply_diff(src, dest)
    assert type(dest["a"]) is MyInt, "Does not replace equals"

    src = {"d": {"a": 1}}
    inner = {}
    dest = {"d": inner}
    apply_diff(src, dest)
    assert dest["d"] is inner, "Updates inner dicts"


def test_apply_diff_seq():
    src = [1]
    dest = [MyInt(1)]
    apply_diff(src, dest)
    assert type(dest[0]) is MyInt, "Does not replace equals"

    src = {"l": [1]}
    inner = []
    dest = {"l": inner}
    apply_diff(src, dest)
    assert dest["l"] is inner, "Updates inner lists"


def is_serializable(d):
    json.dumps(d)
    dumps_yaml(d)
    return True


def test_to_omegaconf():
    class CustomDict(dict):
        pass

    class CustomList(list):
        pass

    data = {
        "foo": CustomDict(bar=1, bag=CustomList([1, 2])),
        "goo": CustomList([CustomDict(goobar=1)]),
    }
    new_data = to_omegaconf(data)
    assert not isinstance(new_data["foo"], CustomDict)
    assert not isinstance(new_data["foo"]["bag"], CustomList)
    assert not isinstance(new_data["goo"], CustomList)
    assert not isinstance(new_data["goo"][0], CustomDict)


@pytest.mark.parametrize(
    "changes, expected",
    [
        [{"foo": "baz"}, {"foo": "baz", "goo": {"bag": 3}, "lorem": False}],
        [
            {"foo": "baz", "goo": "bar"},
            {"foo": "baz", "goo": "bar", "lorem": False},
        ],
        [
            {"goo": {"bag": 4}},
            {"foo": {"bar": 1, "baz": 2}, "goo": {"bag": 4}, "lorem": False},
        ],
        [
            {"foo": {"bar": 1, "baz": 2, 0: "bar"}},
            {
                "foo": {"bar": 1, "baz": 2, 0: "bar"},
                "goo": {"bag": 3},
                "lorem": False,
            },
        ],
        [
            {"lorem": {"ipsum": 3}},
            {
                "foo": {"bar": 1, "baz": 2},
                "goo": {"bag": 3},
                "lorem": {"ipsum": 3},
            },
        ],
        [{}, {"foo": {"bar": 1, "baz": 2}, "goo": {"bag": 3}, "lorem": False}],
    ],
)
def test_merge_dicts(changes, expected):
    params = {"foo": {"bar": 1, "baz": 2}, "goo": {"bag": 3}, "lorem": False}
    merged = merge_dicts(params, changes)
    assert merged == expected == params
    assert params is merged  # references should be preserved
    assert is_serializable(params)


@pytest.mark.parametrize(
    "changes, expected",
    [
        [{"foo": "baz"}, {"foo": {"baz": 2}}],
        [
            {"foo": "baz", "goo": "bag"},
            {"foo": {"baz": 2}, "goo": {"bag": 3}},
        ],
        [{}, {}],
    ],
)
def test_remove_missing_keys(changes, expected):
    params = {"foo": {"bar": 1, "baz": 2}, "goo": {"bag": 3}, "lorem": False}
    removed = remove_missing_keys(params, changes)
    assert removed == expected == params
    assert params is removed  # references should be preserved
    assert is_serializable(params)
