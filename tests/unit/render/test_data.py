from collections import OrderedDict

import pytest

from dvc.render.data import (
    Converter,
    FieldsNotFoundError,
    _filter_fields,
    _find_first_list,
    _lists,
    to_datapoints,
)


def test_find_first_list_in_dict():
    m1 = [{"accuracy": 1, "loss": 2}, {"accuracy": 3, "loss": 4}]
    m2 = [{"x": 1}, {"x": 2}]
    dmetric = OrderedDict([("t1", m1), ("t2", m2)])

    assert _find_first_list(dmetric, fields=set()) == m1
    assert _find_first_list(dmetric, fields={"x"}) == m2


def test_filter_fields():
    m = [{"accuracy": 1, "loss": 2}, {"accuracy": 3, "loss": 4}]

    assert _filter_fields(m, fields=set()) == m

    expected = [{"accuracy": 1}, {"accuracy": 3}]
    assert _filter_fields(m, fields={"accuracy"}) == expected

    with pytest.raises(FieldsNotFoundError):
        _filter_fields(m, fields={"bad_field"})


@pytest.mark.parametrize(
    "dictionary, expected_result",
    [
        ({}, []),
        ({"x": ["a", "b", "c"]}, [["a", "b", "c"]]),
        (
            OrderedDict([("x", {"y": ["a", "b"]}), ("z", {"w": ["c", "d"]})]),
            [["a", "b"], ["c", "d"]],
        ),
    ],
)
def test_finding_lists(dictionary, expected_result):
    result = _lists(dictionary)

    assert list(result) == expected_result


@pytest.mark.parametrize(
    "input_data,properties,expected_datapoints,expected_properties",
    [
        (
            # default x and y
            {"metric": [{"v": 1}, {"v": 2}]},
            {},
            [{"v": 1, "step": 0}, {"v": 2, "step": 1}],
            {"x": "step", "y": "v"},
        ),
        (
            # filter fields
            {"metric": [{"v": 1, "v2": 0.1}, {"v": 2, "v2": 0.2}]},
            {"fields": {"v"}},
            [{"v": 1, "step": 0}, {"v": 2, "step": 1}],
            {
                "x": "step",
                "y": "v",
                "fields": {"v", "step"},
            },
        ),
        (
            # choose x and y
            {"metric": [{"v": 1, "v2": 0.1}, {"v": 2, "v2": 0.2}]},
            {"x": "v", "y": "v2"},
            [{"v": 1, "v2": 0.1}, {"v": 2, "v2": 0.2}],
            {"x": "v", "y": "v2"},
        ),
        (
            # append x and y to filtered fields
            {
                "metric": [
                    {"v": 1, "v2": 0.1, "v3": 0.01, "v4": 0.001},
                    {"v": 2, "v2": 0.2, "v3": 0.02, "v4": 0.002},
                ]
            },
            {"x": "v3", "y": "v4", "fields": {"v"}},
            [
                {"v": 1, "v3": 0.01, "v4": 0.001},
                {"v": 2, "v3": 0.02, "v4": 0.002},
            ],
            {"x": "v3", "y": "v4", "fields": {"v", "v3", "v4"}},
        ),
        (
            # find metric in nested structure
            {
                "some": "noise",
                "very": {
                    "nested": {
                        "metric": [{"v": 1, "v2": 0.1}, {"v": 2, "v2": 0.2}]
                    }
                },
            },
            {"x": "v", "y": "v2"},
            [{"v": 1, "v2": 0.1}, {"v": 2, "v2": 0.2}],
            {"x": "v", "y": "v2"},
        ),
    ],
)
def test_convert(
    input_data, properties, expected_datapoints, expected_properties
):
    converter = Converter(properties)
    datapoints, resolved_properties = converter.convert(input_data)

    assert datapoints == expected_datapoints
    assert resolved_properties == expected_properties


def test_convert_skip_step():
    converter = Converter()
    converter.skip_step("append_index")

    datapoints, resolved_properties = converter.convert(
        {"a": "b", "metric": [{"v": 1}, {"v": 2}]}
    )

    assert datapoints == [{"v": 1}, {"v": 2}]
    assert resolved_properties == {"x": "step", "y": "v"}


def test_to_datapoints():
    input_data = {
        "revision": {
            "data": {
                "filename": {
                    "data": {
                        "metric": [
                            {"v": 1, "v2": 0.1, "v3": 0.01, "v4": 0.001},
                            {"v": 2, "v2": 0.2, "v3": 0.02, "v4": 0.002},
                        ]
                    }
                }
            }
        }
    }
    props = {"fields": {"v"}, "x": "v2", "y": "v3"}

    datapoints, resolved_properties = to_datapoints(input_data, props)

    assert datapoints == [
        {"v": 1, "v2": 0.1, "v3": 0.01, "rev": "revision"},
        {"v": 2, "v2": 0.2, "v3": 0.02, "rev": "revision"},
    ]
    assert resolved_properties == {
        "fields": {"v", "v2", "v3"},
        "x": "v2",
        "y": "v3",
    }
