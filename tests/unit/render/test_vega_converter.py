from collections import OrderedDict

import pytest

from dvc.render import VERSION_FIELD
from dvc.render.converter.vega import VegaConverter, _lists


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
        pytest.param(
            {"f": {"metric": [{"v": 1}, {"v": 2}]}},
            {},
            [
                {
                    "v": 1,
                    "step": 0,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v",
                    },
                },
                {
                    "v": 2,
                    "step": 1,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v",
                    },
                },
            ],
            {"x": "step", "y": "v", "x_label": "step", "y_label": "v"},
            id="default_x_y",
        ),
        pytest.param(
            {"f": {"metric": [{"v": 1, "v2": 0.1}, {"v": 2, "v2": 0.2}]}},
            {"x": "v", "y": "v2"},
            [
                {
                    "v": 1,
                    "v2": 0.1,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v2",
                    },
                },
                {
                    "v": 2,
                    "v2": 0.2,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v2",
                    },
                },
            ],
            {"x": "v", "y": "v2", "x_label": "v", "y_label": "v2"},
            id="choose_x_y",
        ),
        pytest.param(
            {
                "f": {
                    "some": "noise",
                    "very": {
                        "nested": {
                            "metric": [
                                {"v": 1, "v2": 0.1},
                                {"v": 2, "v2": 0.2},
                            ]
                        }
                    },
                }
            },
            {"x": "v", "y": "v2", "x_label": "x", "y_label": "y"},
            [
                {
                    "v": 1,
                    "v2": 0.1,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v2",
                    },
                },
                {
                    "v": 2,
                    "v2": 0.2,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v2",
                    },
                },
            ],
            {"x": "v", "y": "v2", "x_label": "x", "y_label": "y"},
            id="find_in_nested_structure",
        ),
        pytest.param(
            {"f": {"metric": [{"v": 1, "v2": 0.1}, {"v": 2, "v2": 0.2}]}},
            {"y": {"f": ["v", "v2"]}},
            [
                {
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v",
                    },
                    "dvc_inferred_y_value": 1,
                    "v": 1,
                    "v2": 0.1,
                    "step": 0,
                },
                {
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v",
                    },
                    "dvc_inferred_y_value": 2,
                    "v": 2,
                    "v2": 0.2,
                    "step": 1,
                },
                {
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v2",
                    },
                    "dvc_inferred_y_value": 0.1,
                    "v2": 0.1,
                    "v": 1,
                    "step": 0,
                },
                {
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v2",
                    },
                    "v": 2,
                    "v2": 0.2,
                    "dvc_inferred_y_value": 0.2,
                    "step": 1,
                },
            ],
            {
                "x": "step",
                "y": "dvc_inferred_y_value",
                "y_label": "y",
                "x_label": "step",
            },
            id="y_def_list",
        ),
        pytest.param(
            {
                "f": {
                    "metric": [{"v": 1}, {"v": 2}],
                    "other_metric": [{"z": 3}, {"z": 4}],
                }
            },
            {"y": {"f": ["v", "z"]}},
            [
                {
                    "dvc_inferred_y_value": 1,
                    "z": 3,
                    "v": 1,
                    "step": 0,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v",
                    },
                },
                {
                    "dvc_inferred_y_value": 2,
                    "z": 4,
                    "step": 1,
                    "v": 2,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v",
                    },
                },
                {
                    "dvc_inferred_y_value": 3,
                    "v": 1,
                    "z": 3,
                    "step": 0,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "z",
                    },
                },
                {
                    "dvc_inferred_y_value": 4,
                    "v": 2,
                    "z": 4,
                    "step": 1,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "z",
                    },
                },
            ],
            {
                "x": "step",
                "y": "dvc_inferred_y_value",
                "y_label": "y",
                "x_label": "step",
            },
            id="multi_source_json",
        ),
    ],
)
def test_convert(
    input_data,
    properties,
    expected_datapoints,
    expected_properties,
):
    converter = VegaConverter("f", input_data, properties)
    datapoints, resolved_properties = converter.flat_datapoints("r")

    assert datapoints == expected_datapoints
    assert resolved_properties == expected_properties


@pytest.mark.parametrize(
    "properties,label",
    [
        ({"x": {"actual.csv": "actual"}}, "actual"),
        (
            {"x": {"train_actual.csv": "actual", "val_actual.csv": "actual"}},
            "actual",
        ),
        (
            {"x": {"actual.csv": "actual", "predicted.csv": "predicted"}},
            "x",
        ),
    ],
)
def test_infer_x_label(properties, label):
    assert VegaConverter.infer_x_label(properties) == label
