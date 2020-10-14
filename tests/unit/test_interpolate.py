from math import inf, pi

import pytest

from dvc.parsing import Context
from dvc.parsing.interpolate import resolve


@pytest.mark.parametrize(
    "template, var",
    [
        ("${value}", "value"),
        ("${{item}}", "item"),
        ("${ item }", "item"),
        ("${{ value }}", "value"),
    ],
)
@pytest.mark.parametrize(
    "data", [True, 12, pi, None, False, 0, "0", "123", "Foobar", "", inf, 3e4]
)
def test_resolve_primitive_values(data, template, var):
    assert resolve(template, Context({var: data})) == data


@pytest.mark.parametrize(
    "template, expected",
    [
        (r"\${value}", "${value}"),
        (r"\${{value}}", "${{value}}"),
        (r"\${ value }", "${ value }"),
        (r"\${{ value }}", "${{ value }}"),
        (r"\${{ value }} days", "${{ value }} days"),
        (r"\${ value } days", "${ value } days"),
        (r"Month of \${value}", "Month of ${value}"),
        (r"May the \${value} be with you", "May the ${value} be with you"),
        (
            r"Great shot kid, that was \${value} in a ${value}",
            "Great shot kid, that was ${value} in a value",
        ),
    ],
)
def test_escape(template, expected, mocker):
    assert resolve(template, Context({"value": "value"})) == expected


def test_resolve_str():
    template = "My name is ${last}, ${first} ${last}"
    expected = "My name is Bond, James Bond"
    assert (
        resolve(template, Context({"first": "James", "last": "Bond"}))
        == expected
    )


def test_resolve_primitives_dict_access():
    data = {
        "dict": {
            "num": 5,
            "string": "foo",
            "nested": {"float": pi, "string": "bar"},
        }
    }
    context = Context(data)

    assert resolve("${dict.num}", context) == 5
    assert resolve("${dict.string}", context) == "foo"
    assert resolve("${dict.nested.float}", context) == pi
    assert resolve("${dict.nested.string}", context) == "bar"

    assert resolve("Number ${dict.num}", context) == "Number 5"


def test_resolve_primitives_list_access():
    context = Context(
        {
            "dict": [
                {"f": "f"},
                {"fo": "fo"},
                {"foo": "foo"},
                {"foo": ["f", "o", "o"]},
            ]
        }
    )

    assert resolve("${dict.0.f}", context) == "f"
    assert resolve("${dict.1.fo}", context) == "fo"
    assert resolve("${dict.2.foo}", context) == "foo"
    assert resolve("${dict.3.foo.0}", context) == "f"

    assert resolve("${ dict.1.fo}${dict.3.foo.1}bar", context) == "foobar"


def test_resolve_collection():
    from .test_stage_resolver import (
        CONTEXT_DATA,
        RESOLVED_DVC_YAML_DATA,
        TEMPLATED_DVC_YAML_DATA,
    )

    context = Context(CONTEXT_DATA)
    assert resolve(TEMPLATED_DVC_YAML_DATA, context) == RESOLVED_DVC_YAML_DATA
