from collections import OrderedDict

import pytest

from dvc.exceptions import DvcException
from dvc.render import VERSION_FIELD
from dvc.render.converter.vega import FieldNotFoundError, VegaConverter, _lists


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
        pytest.param(
            {
                "f": {"metric": [{"v": 1, "v2": 0.1}, {"v": 2, "v2": 0.2}]},
                "f2": {"metric": [{"v": 3, "v2": 0.3}]},
            },
            {"x": "v", "y": {"f": "v2", "f2": "v2"}},
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
                {
                    "v": 3,
                    "v2": 0.3,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f2",
                        "field": "v2",
                    },
                },
            ],
            {"x": "v", "y": "v2", "x_label": "v", "y_label": "v2"},
            id="multi_file_json",
        ),
        pytest.param(
            {"f": {"metric": [{"v": 1, "v2": 0.1}, {"v": 2, "v2": 0.2}]}},
            {"y": ["v", "v2"]},
            [
                {
                    "dvc_inferred_y_value": 1,
                    "v": 1,
                    "v2": 0.1,
                    "step": 0,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v",
                    },
                },
                {
                    "dvc_inferred_y_value": 2,
                    "v": 2,
                    "v2": 0.2,
                    "step": 1,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v",
                    },
                },
                {
                    "dvc_inferred_y_value": 0.1,
                    "v": 1,
                    "v2": 0.1,
                    "step": 0,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v2",
                    },
                },
                {
                    "dvc_inferred_y_value": 0.2,
                    "v": 2,
                    "v2": 0.2,
                    "step": 1,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v2",
                    },
                },
            ],
            {
                "x": "step",
                "y": "dvc_inferred_y_value",
                "x_label": "step",
                "y_label": "y",
            },
            id="y_list",
        ),
        pytest.param(
            {
                "f": {"metric": [{"v": 1, "v2": 0.1, "v3": 0.01}]},
                "f2": {"metric": [{"v": 1, "v2": 0.1}]},
            },
            {"y": {"f": ["v2", "v3"], "f2": ["v2"]}, "x": "v"},
            [
                {
                    "dvc_inferred_y_value": 0.1,
                    "v": 1,
                    "v2": 0.1,
                    "v3": 0.01,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v2",
                    },
                },
                {
                    "dvc_inferred_y_value": 0.01,
                    "v": 1,
                    "v2": 0.1,
                    "v3": 0.01,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f",
                        "field": "v3",
                    },
                },
                {
                    "dvc_inferred_y_value": 0.1,
                    "v": 1,
                    "v2": 0.1,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f2",
                        "field": "v2",
                    },
                },
            ],
            {
                "x": "v",
                "y": "dvc_inferred_y_value",
                "x_label": "v",
                "y_label": "y",
            },
            id="multi_source_y_single_x",
        ),
        pytest.param(
            {
                "dir/f": {"metric": [{"v": 1, "v2": 0.1}]},
                "dir/f2": {"metric": [{"v": 1, "v2": 0.1}]},
            },
            {"y": {"dir/f": ["v2"], "dir/f2": ["v2"]}, "x": "v"},
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
                    "v": 1,
                    "v2": 0.1,
                    VERSION_FIELD: {
                        "revision": "r",
                        "filename": "f2",
                        "field": "v2",
                    },
                },
            ],
            {
                "x": "v",
                "y": "v2",
                "x_label": "v",
                "y_label": "v2",
            },
            id="multi_file_y_same_prefix",
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
    "input_data,properties,exc",
    [
        pytest.param(
            {
                "f": {
                    "metric": [
                        {"v": 1},
                        {"v": 2},
                    ]
                },
                "f2": {"metric": [{"v2": 0.1}]},
            },
            {"x": {"f": "v"}, "y": {"f2": "v2"}},
            DvcException,
            id="unequal_datapoints",
        ),
        pytest.param(
            {
                "f": {
                    "metric": [
                        {"v": 1, "v2": 0.1},
                        {"v": 2, "v2": 0.2},
                    ]
                },
                "f2": {
                    "metric": [
                        {"v": 3, "v2": 0.3},
                    ]
                },
            },
            {"x": {"f": "v", "f2": "v3"}, "y": {"f": "v2"}},
            FieldNotFoundError,
            id="unequal_x_y",
        ),
    ],
)
def test_convert_fail(input_data, properties, exc):
    converter = VegaConverter("f", input_data, properties)
    with pytest.raises(exc):
        converter.flat_datapoints("r")


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
