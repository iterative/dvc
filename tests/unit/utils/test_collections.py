# pylint: disable=unidiomatic-typecheck
import json
from json import encoder
from unittest.mock import create_autospec

import pytest

from dvc.utils.collections import (
    apply_diff,
    chunk_dict,
    merge_params,
    validate,
)
from dvc.utils.serialize import dumps_yaml


class MyDict(dict):
    pass


class MyInt(int):
    pass


def test_apply_diff_is_inplace():
    dest = MyDict()
    dest.attr = 42  # pylint: disable=attribute-defined-outside-init
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


def test_chunk_dict():
    assert chunk_dict({}) == []

    d = {"a": 1, "b": 2, "c": 3}
    assert chunk_dict(d) == [{"a": 1}, {"b": 2}, {"c": 3}]
    assert chunk_dict(d, 2) == [{"a": 1, "b": 2}, {"c": 3}]
    assert chunk_dict(d, 3) == [d]
    assert chunk_dict(d, 4) == [d]


# pylint: disable=unused-argument


def _test_func(x, y, *args, j=3, k=5, **kwargs):
    pass


def test_pre_validate_decorator_required_args(mocker):
    mock = mocker.MagicMock()

    func_mock = create_autospec(_test_func)
    func = validate(mock)(func_mock)

    func("x", "y")

    func_mock.assert_called_once_with("x", "y", j=3, k=5)
    mock.assert_called_once()

    (args,) = mock.call_args[0]
    assert args.x == "x"
    assert args.y == "y"
    assert args.args == ()
    assert args.j == 3
    assert args.k == 5
    assert args.kwargs == {}


def test_pre_validate_decorator_kwargs_args(mocker):
    mock = mocker.MagicMock()
    func_mock = create_autospec(_test_func)
    func = validate(mock)(func_mock)

    func("x", "y", "part", "of", "args", j=1, k=10, m=100, n=1000)

    func_mock.assert_called_once_with(
        "x", "y", "part", "of", "args", j=1, k=10, m=100, n=1000
    )
    mock.assert_called_once()
    (args,) = mock.call_args[0]
    assert args.x == "x"
    assert args.y == "y"
    assert args.args == ("part", "of", "args")
    assert args.j == 1
    assert args.k == 10
    assert args.kwargs == {"m": 100, "n": 1000}


def test_pre_validate_update_args():
    def test_validator(args):
        args.w += 50
        del args.x
        args.y = 100

    def test_func(w=1, x=5, y=10, z=15):
        pass

    mock = create_autospec(test_func)
    func = validate(test_validator)(mock)

    func(100, 100)
    mock.assert_called_once_with(w=150, y=100, z=15)


def test_post_validate_decorator(mocker):
    def none_filter(result):
        return list(filter(None, result))

    test_func = mocker.MagicMock(return_value=[1, None, 2])
    func = validate(none_filter, post=True)(test_func)

    result = func()
    test_func.assert_called_once()
    assert result == [1, 2]


def is_serializable(d):
    json.dumps(d)
    dumps_yaml(d)
    return True


@pytest.mark.parametrize(
    "changes, expected",
    [
        [{"foo": "baz"}, {"foo": "baz", "goo": {"bag": 3}, "lorem": False}],
        [
            {"foo": "baz", "goo": "bar"},
            {"foo": "baz", "goo": "bar", "lorem": False},
        ],
        [
            {"goo.bag": 4},
            {"foo": {"bar": 1, "baz": 2}, "goo": {"bag": 4}, "lorem": False},
        ],
        [
            {"foo[0]": "bar"},
            {
                "foo": {"bar": 1, "baz": 2, 0: "bar"},
                "goo": {"bag": 3},
                "lorem": False,
            },
        ],
        [
            {"foo[1].baz": 3},
            {
                "foo": {"bar": 1, "baz": 2, 1: {"baz": 3}},
                "goo": {"bag": 3},
                "lorem": False,
            },
        ],
        [
            {"foo[1]": ["baz", "goo"]},
            {
                "foo": {"bar": 1, "baz": 2, 1: ["baz", "goo"]},
                "goo": {"bag": 3},
                "lorem": False,
            },
        ],
        [
            {"lorem.ipsum": 3},
            {
                "foo": {"bar": 1, "baz": 2},
                "goo": {"bag": 3},
                "lorem": {"ipsum": 3},
            },
        ],
        [{}, {"foo": {"bar": 1, "baz": 2}, "goo": {"bag": 3}, "lorem": False}],
    ],
)
def test_merge_params(changes, expected):
    params = {"foo": {"bar": 1, "baz": 2}, "goo": {"bag": 3}, "lorem": False}
    merged = merge_params(params, changes)
    assert merged == expected == params
    assert params is merged  # references should be preserved
    assert encoder.c_make_encoder
    assert is_serializable(params)


@pytest.mark.parametrize(
    "changes, expected",
    [
        [{"foo": "baz"}, {"foo": "baz"}],
        [{"foo": "baz", "goo": "bar"}, {"foo": "baz", "goo": "bar"}],
        [{"foo[1]": ["baz", "goo"]}, {"foo": [None, ["baz", "goo"]]}],
        [{"foo.bar": "baz"}, {"foo": {"bar": "baz"}}],
    ],
)
def test_merge_params_on_empty_src(changes, expected):
    params = {}
    merged = merge_params(params, changes)
    assert merged == expected == params
    assert params is merged  # references should be preserved
    assert encoder.c_make_encoder
    assert is_serializable(params)


def test_benedict_rollback_its_monkeypatch():
    from dvc.utils._benedict import benedict

    assert benedict({"foo": "foo"}) == {"foo": "foo"}
    assert encoder.c_make_encoder
