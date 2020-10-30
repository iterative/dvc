from dataclasses import asdict
from math import pi

import pytest

from dvc.parsing.context import Context, CtxDict, CtxList, Value
from dvc.tree.local import LocalTree
from dvc.utils.serialize import dump_yaml


def test_context():
    context = Context({"foo": "bar"})
    assert context["foo"] == Value("bar")

    context = Context(foo="bar")
    assert context["foo"] == Value("bar")

    context["foobar"] = "foobar"
    assert context["foobar"] == Value("foobar")

    del context["foobar"]
    assert "foobar" not in context
    assert "foo" in context

    with pytest.raises(KeyError):
        _ = context["foobar"]


def test_context_dict_ignores_keys_except_str():
    c = Context({"one": 1, 3: 3})
    assert "one" in c
    assert 3 not in c

    c[3] = 3
    assert 3 not in c


def test_context_list():
    lst = ["foo", "bar", "baz"]
    context = Context(lst=lst)

    assert context["lst"] == CtxList(lst)
    assert context["lst"][0] == Value("foo")
    del context["lst"][-1]

    assert "baz" not in context

    with pytest.raises(IndexError):
        _ = context["lst"][3]

    context["lst"].insert(0, "baz")
    assert context["lst"] == CtxList(["baz"] + lst[:2])


def test_context_setitem_getitem():
    context = Context()
    lst = [1, 2, "three", True, pi, b"bytes", None]
    context["list"] = lst

    assert isinstance(context["list"], CtxList)
    assert context["list"] == CtxList(lst)
    for i, val in enumerate(lst):
        assert context["list"][i] == Value(val)

    d = {
        "foo": "foo",
        "bar": "bar",
        "list": [
            {"foo0": "foo0", "bar0": "bar0"},
            {"foo1": "foo1", "bar1": "bar1"},
        ],
    }
    context["data"] = d

    assert isinstance(context["data"], CtxDict)
    assert context["data"] == CtxDict(d)
    assert context["data"]["foo"] == Value("foo")
    assert context["data"]["bar"] == Value("bar")

    assert isinstance(context["data"]["list"], CtxList)
    assert context["data"]["list"] == CtxList(d["list"])

    for i, val in enumerate(d["list"]):
        c = context["data"]["list"][i]
        assert isinstance(c, CtxDict)
        assert c == CtxDict(val)
        assert c[f"foo{i}"] == Value(f"foo{i}")
        assert c[f"bar{i}"] == Value(f"bar{i}")

    with pytest.raises(TypeError):
        context["set"] = {1, 2, 3}


def test_loop_context():
    context = Context({"foo": "foo", "bar": "bar", "lst": [1, 2, 3]})

    assert list(context) == ["foo", "bar", "lst"]
    assert len(context) == 3

    assert list(context["lst"]) == [Value(i) for i in [1, 2, 3]]
    assert len(context["lst"]) == 3

    assert list(context.items()) == [
        ("foo", Value("foo")),
        ("bar", Value("bar")),
        ("lst", CtxList([1, 2, 3])),
    ]


def test_repr():
    data = {"foo": "foo", "bar": "bar", "lst": [1, 2, 3]}
    context = Context(data)

    assert repr(context) == repr(data)
    assert str(context) == str(data)


def test_select():
    context = Context(foo="foo", bar="bar", lst=[1, 2, 3])

    assert context.select("foo") == Value("foo")
    assert context.select("bar") == Value("bar")
    assert context.select("lst") == CtxList([1, 2, 3])
    assert context.select("lst.0") == Value(1)

    with pytest.raises(ValueError):
        context.select("baz")

    d = {
        "lst": [
            {"foo0": "foo0", "bar0": "bar0"},
            {"foo1": "foo1", "bar1": "bar1"},
        ]
    }
    context = Context(d)
    assert context.select("lst") == CtxList(d["lst"])
    assert context.select("lst.0") == CtxDict(d["lst"][0])
    assert context.select("lst.1") == CtxDict(d["lst"][1])

    with pytest.raises(ValueError):
        context.select("lst.2")

    for i, _ in enumerate(d["lst"]):
        assert context.select(f"lst.{i}.foo{i}") == Value(f"foo{i}")
        assert context.select(f"lst.{i}.bar{i}") == Value(f"bar{i}")


def test_merge_dict():
    d1 = {"Train": {"us": {"lr": 10}}}
    d2 = {"Train": {"us": {"layers": 100}}}

    c1 = Context(d1)
    c2 = Context(d2)

    c1.merge_update(c2)
    assert c1.select("Train.us") == CtxDict(lr=10, layers=100)

    with pytest.raises(ValueError):
        # cannot overwrite by default
        c1.merge_update({"Train": {"us": {"lr": 15}}})

    c1.merge_update({"Train": {"us": {"lr": 15}}}, overwrite=True)
    assert c1.select("Train.us") == CtxDict(lr=15, layers=100)


def test_merge_list():
    c1 = Context(lst=[1, 2, 3])
    with pytest.raises(ValueError):
        # cannot overwrite by default
        c1.merge_update({"lst": [10, 11, 12]})

    # lists are never merged
    c1.merge_update({"lst": [10, 11, 12]}, overwrite=True)
    assert c1.select("lst") == [10, 11, 12]


def test_overwrite_with_setitem():
    context = Context(foo="foo", d={"bar": "bar", "baz": "baz"})
    context["d"] = "overwrite"
    assert "d" in context
    assert context["d"] == Value("overwrite")


def test_load_from(mocker):
    def _yaml_load(*args, **kwargs):
        return {"x": {"y": {"z": 5}, "lst": [1, 2, 3]}, "foo": "foo"}

    mocker.patch("dvc.parsing.context.LOADERS", {".yaml": _yaml_load})
    file = "params.yaml"
    c = Context.load_from(object(), file)

    assert asdict(c["x"].meta) == {"source": file, "dpaths": ["x"]}
    assert asdict(c["foo"].meta) == {"source": file, "dpaths": ["foo"]}
    assert asdict(c["x"]["y"].meta) == {"source": file, "dpaths": ["x", "y"]}
    assert asdict(c["x"]["y"]["z"].meta) == {
        "source": file,
        "dpaths": ["x", "y", "z"],
    }
    assert asdict(c["x"]["lst"].meta) == {
        "source": file,
        "dpaths": ["x", "lst"],
    }
    assert asdict(c["x"]["lst"][0].meta) == {
        "source": file,
        "dpaths": ["x", "lst", "0"],
    }


def test_clone():
    d = {
        "lst": [
            {"foo0": "foo0", "bar0": "bar0"},
            {"foo1": "foo1", "bar1": "bar1"},
        ]
    }
    c1 = Context(d)
    c2 = Context.clone(c1)

    c2["lst"][0]["foo0"] = "foo"
    del c2["lst"][1]["foo1"]

    assert c1 != c2
    assert c1 == Context(d)
    assert c2.select("lst.0.foo0") == Value("foo")
    with pytest.raises(ValueError):
        c2.select("lst.1.foo1")


def test_track(tmp_dir):
    d = {
        "lst": [
            {"foo0": "foo0", "bar0": "bar0"},
            {"foo1": "foo1", "bar1": "bar1"},
        ],
        "dct": {"foo": "foo", "bar": "bar", "baz": "baz"},
    }
    tree = LocalTree(None, config={})
    path = tmp_dir / "params.yaml"
    dump_yaml(path, d, tree)

    context = Context.load_from(tree, str(path))

    def key_tracked(key):
        assert len(context.tracked) == 1
        return key in context.tracked[str(path)]

    with context.track():
        context.select("lst")
        assert key_tracked("lst")

        context.select("dct")
        assert not key_tracked("dct")

        context.select("dct.foo")
        assert key_tracked("dct.foo")

        # Currently, it's unable to track dictionaries, as it can be merged
        # from multiple sources.
        context.select("lst.0")
        assert not key_tracked("lst.0")

        # FIXME: either support tracking list values in ParamsDependency
        # or, prevent this from being tracked.
        context.select("lst.0.foo0")
        assert key_tracked("lst.0.foo0")


def test_track_from_multiple_files(tmp_dir):
    d1 = {"Train": {"us": {"lr": 10}}}
    d2 = {"Train": {"us": {"layers": 100}}}

    tree = LocalTree(None, config={})
    path1 = tmp_dir / "params.yaml"
    path2 = tmp_dir / "params2.yaml"
    dump_yaml(path1, d1, tree)
    dump_yaml(path2, d2, tree)

    context = Context.load_from(tree, str(path1))
    c = Context.load_from(tree, str(path2))
    context.merge_update(c)

    def key_tracked(path, key):
        return key in context.tracked[str(path)]

    with context.track():
        context.select("Train")
        assert not (key_tracked(path1, "Train") or key_tracked(path2, "Train"))

        context.select("Train.us")
        assert not (
            key_tracked(path1, "Train.us") or key_tracked(path2, "Train.us")
        )

        context.select("Train.us.lr")
        assert key_tracked(path1, "Train.us.lr") and not key_tracked(
            path2, "Train.us.lr"
        )
        context.select("Train.us.layers")
        assert not key_tracked(path1, "Train.us.layers") and key_tracked(
            path2, "Train.us.layers"
        )

    context = Context.clone(context)
    assert not context.tracked

    # let's see with an alias
    context["us"] = context["Train"]["us"]
    with context.track():
        context.select("us")
        assert not (
            key_tracked(path1, "Train.us") or key_tracked(path2, "Train.us")
        )

        context.select("us.lr")
        assert key_tracked(path1, "Train.us.lr") and not key_tracked(
            path2, "Train.us.lr"
        )
        context.select("Train.us.layers")
        assert not key_tracked(path1, "Train.us.layers") and key_tracked(
            path2, "Train.us.layers"
        )
