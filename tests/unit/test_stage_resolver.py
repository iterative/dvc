from math import pi

import pytest

from dvc.parsing import DataResolver, ResolveError
from dvc.parsing.context import Context, Value

TEMPLATED_DVC_YAML_DATA = {
    "stages": {
        "stage1": {
            "cmd": "python script.py ${dict.foo} --out ${dict.bar}",
            "outs": ["${dict.bar}"],
            "deps": ["${dict.foo}"],
            "params": ["${list[0]}", "${list[1]}"],
            "frozen": "${freeze}",
        },
        "stage2": {"cmd": "echo ${dict.foo} ${dict.bar}"},
    }
}

CONTEXT_DATA = {
    "dict": {"foo": "foo", "bar": "bar"},
    "list": ["param1", "param2"],
    "freeze": True,
}

RESOLVED_DVC_YAML_DATA = {
    "stages": {
        "stage1": {
            "cmd": "python script.py foo --out bar",
            "outs": ["bar"],
            "deps": ["foo"],
            "params": ["param1", "param2"],
            "frozen": True,
        },
        "stage2": {"cmd": "echo foo bar"},
    }
}


def test_resolver(tmp_dir, dvc):
    resolver = DataResolver(dvc, tmp_dir, TEMPLATED_DVC_YAML_DATA)
    resolver.global_ctx = Context(CONTEXT_DATA)
    assert resolver.resolve() == RESOLVED_DVC_YAML_DATA


def test_set():
    context = Context(CONTEXT_DATA)
    to_set = {
        "foo": "foo",
        "bar": "bar",
        "pi": pi,
        "true": True,
        "false": False,
        "none": "None",
        "int": 1,
        "lst2": [1, 2, 3],
        "dct2": {"foo": "bar", "foobar": "foobar"},
    }
    DataResolver.set_context_from(context, to_set)

    for key, value in to_set.items():
        # FIXME: using for convenience, figure out better way to do it
        assert context[key] == context._convert(key, value)


@pytest.mark.parametrize(
    "coll",
    [
        ["foo", "bar", ["foo", "bar"]],
        ["foo", "bar", {"foo": "foo", "bar": "bar"}],
        {"foo": "foo", "bar": ["foo", "bar"]},
        {"foo": "foo", "bar": {"foo": "foo", "bar": "bar"}},
    ],
)
def test_set_nested_coll(coll):
    context = Context(CONTEXT_DATA)
    with pytest.raises(ResolveError) as exc_info:
        DataResolver.set_context_from(context, {"thresh": 10, "item": coll})

    assert (
        str(exc_info.value) == "Failed to set 'item': Cannot set 'item', "
        "has nested dict/list"
    )


def test_set_already_exists():
    context = Context({"item": "foo"})
    with pytest.raises(ResolveError) as exc_info:
        DataResolver.set_context_from(context, {"item": "bar"})

    assert (
        str(exc_info.value) == "Failed to set 'item': Cannot set 'item', "
        "key already exists"
    )
    assert context["item"] == Value("foo")


@pytest.mark.parametrize(
    "coll", [["foo", "${bar}"], {"foo": "${foo}", "bar": "bar"}],
)
def test_set_collection_interpolation(coll):
    context = Context(CONTEXT_DATA)
    with pytest.raises(ResolveError) as exc_info:
        DataResolver.set_context_from(context, {"thresh": 10, "item": coll})

    assert (
        str(exc_info.value) == "Failed to set 'item': Cannot set 'item', "
        f"having interpolation inside '{type(coll).__name__}' "
        "is not supported."
    )


def test_set_interpolated_string():
    context = Context(CONTEXT_DATA)
    DataResolver.set_context_from(
        context,
        {
            "foo": "${dict.foo}",
            "bar": "${dict.bar}",
            "param1": "${list[0]}",
            "param2": "${list[1]}",
            "frozen": "${freeze}",
            "dict2": "${dict}",
            "list2": "${list}",
        },
    )

    assert context["foo"] == Value("foo")
    assert context["bar"] == Value("bar")
    assert context["param1"] == Value("param1")
    assert context["param2"] == Value("param2")
    assert context["frozen"] == context["freeze"] == Value(True)
    assert context["dict2"] == context["dict"] == CONTEXT_DATA["dict"]
    assert context["list2"] == context["list"] == CONTEXT_DATA["list"]


def test_set_ladder():
    context = Context(CONTEXT_DATA)
    DataResolver.set_context_from(
        context,
        {
            "item": 5,
            "foo": "${dict.foo}",
            "bar": "${dict.bar}",
            "bar2": "${bar}",
            "dict2": "${dict}",
            "list2": "${list}",
            "dict3": "${dict2}",
            "list3": "${list2}",
        },
    )

    assert context["item"] == Value(5)
    assert context["foo"] == context["dict"]["foo"] == Value("foo")
    assert (
        context["bar"]
        == context["bar2"]
        == context["dict"]["bar"]
        == Value("bar")
    )
    assert (
        context["dict"]
        == context["dict2"]
        == context["dict3"]
        == CONTEXT_DATA["dict"]
    )
    assert (
        context["list"]
        == context["list2"]
        == context["list3"]
        == CONTEXT_DATA["list"]
    )


@pytest.mark.parametrize(
    "value",
    ["param ${dict.foo}", "${dict.bar}${dict.foo}", "${dict.foo}-${dict.bar}"],
)
def test_set_multiple_interpolations(value):
    context = Context(CONTEXT_DATA)
    with pytest.raises(ResolveError,) as exc_info:
        DataResolver.set_context_from(context, {"thresh": 10, "item": value})

    assert str(exc_info.value) == (
        "Failed to set 'item': Cannot set 'item', "
        "joining string with interpolated string is not supported"
    )
