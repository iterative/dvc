from collections import OrderedDict

import pytest

from dvc.render.vega_converter import (
    FieldsNotFoundError,
    PlotDataStructureError,
    VegaConverter,
    _filter_fields,
    _find_first_list,
    _lists,
)


def test_find_first_list_in_dict():
    m1 = [{"accuracy": 1, "loss": 2}, {"accuracy": 3, "loss": 4}]
    m2 = [{"x": 1}, {"x": 2}]
    dmetric = OrderedDict([("t1", m1), ("t2", m2)])

    assert _find_first_list(dmetric, fields=set()) == m1
    assert _find_first_list(dmetric, fields={"x"}) == m2

    with pytest.raises(PlotDataStructureError):
        _find_first_list(dmetric, fields={"foo"})


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
            [
                {"v": 1, "step": 0, "filename": "f", "rev": "r"},
                {"v": 2, "step": 1, "filename": "f", "rev": "r"},
            ],
            {"x": "step", "y": "v"},
        ),
        (
            # filter fields
            {"metric": [{"v": 1, "v2": 0.1}, {"v": 2, "v2": 0.2}]},
            {"fields": {"v"}},
            [
                {"v": 1, "step": 0, "filename": "f", "rev": "r"},
                {"v": 2, "step": 1, "filename": "f", "rev": "r"},
            ],
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
            [
                {"v": 1, "v2": 0.1, "filename": "f", "rev": "r"},
                {"v": 2, "v2": 0.2, "filename": "f", "rev": "r"},
            ],
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
                {"v": 1, "v3": 0.01, "v4": 0.001, "filename": "f", "rev": "r"},
                {"v": 2, "v3": 0.02, "v4": 0.002, "filename": "f", "rev": "r"},
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
            [
                {"v": 1, "v2": 0.1, "filename": "f", "rev": "r"},
                {"v": 2, "v2": 0.2, "filename": "f", "rev": "r"},
            ],
            {"x": "v", "y": "v2"},
        ),
    ],
)
def test_convert(
    input_data,
    properties,
    expected_datapoints,
    expected_properties,
):
    converter = VegaConverter(properties)
    datapoints, resolved_properties = converter.convert(
        data=input_data, revision="r", filename="f"
    )

    assert datapoints == expected_datapoints
    assert resolved_properties == expected_properties


def test_convert_skip_step():
    converter = VegaConverter()
    converter.skip_step("append_index")

    datapoints, resolved_properties = converter.convert(
        data={"a": "b", "metric": [{"v": 1}, {"v": 2}]},
        revision="r",
        filename="f",
    )

    assert datapoints == [
        {"v": 1, "filename": "f", "rev": "r"},
        {"v": 2, "filename": "f", "rev": "r"},
    ]
    assert resolved_properties == {"x": "step", "y": "v"}
