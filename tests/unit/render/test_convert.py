from dvc.render import (
    INDEX_FIELD,
    REVISION_FIELD,
    REVISIONS_KEY,
    SRC_FIELD,
    TYPE_KEY,
)
from dvc.render.convert import to_datapoints, to_json


def test_to_datapoints_single_revision(mocker):
    renderer = mocker.MagicMock()
    renderer.TYPE = "vega"

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

    datapoints, resolved_properties = to_datapoints(
        renderer, input_data, props
    )

    assert datapoints == [
        {
            "v": 1,
            "v2": 0.1,
            "v3": 0.01,
            "rev": "revision",
            "filename": "filename",
        },
        {
            "v": 2,
            "v2": 0.2,
            "v3": 0.02,
            "rev": "revision",
            "filename": "filename",
        },
    ]
    assert resolved_properties == {
        "fields": {"v", "v2", "v3"},
        "x": "v2",
        "y": "v3",
    }


def test_to_datapoints_revision_with_error(mocker):
    renderer = mocker.MagicMock()
    renderer.TYPE = "vega"

    data = {
        "v2": {
            "data": {"file.json": {"data": [{"y": 2}, {"y": 3}], "props": {}}}
        },
        "workspace": {
            "data": {"file.json": {"error": FileNotFoundError(), "props": {}}}
        },
    }
    datapoints, final_props = to_datapoints(renderer, data, {})

    assert datapoints == [
        {
            "y": 2,
            INDEX_FIELD: 0,
            REVISION_FIELD: "v2",
            "filename": "file.json",
        },
        {
            "y": 3,
            INDEX_FIELD: 1,
            REVISION_FIELD: "v2",
            "filename": "file.json",
        },
    ]
    assert final_props == {"x": INDEX_FIELD, "y": "y"}


def test_to_datapoints_multiple_revisions(mocker):
    renderer = mocker.MagicMock()
    renderer.TYPE = "vega"

    metric_1 = [{"y": 2}, {"y": 3}]
    metric_2 = [{"y": 3}, {"y": 5}]
    metric_3 = [{"y": 5}, {"y": 6}]

    data = {
        "HEAD": {
            "data": {
                "file.json": {"data": metric_3, "props": {"fields": {"y"}}}
            }
        },
        "v2": {
            "data": {
                "file.json": {"data": metric_2, "props": {"fields": {"y"}}}
            }
        },
        "v1": {
            "data": {
                "file.json": {"data": metric_1, "props": {"fields": {"y"}}}
            }
        },
    }
    props = {"fields": {"y"}}

    datapoints, final_props = to_datapoints(renderer, data, props)

    assert datapoints == [
        {
            "y": 5,
            INDEX_FIELD: 0,
            REVISION_FIELD: "HEAD",
            "filename": "file.json",
        },
        {
            "y": 6,
            INDEX_FIELD: 1,
            REVISION_FIELD: "HEAD",
            "filename": "file.json",
        },
        {
            "y": 3,
            INDEX_FIELD: 0,
            REVISION_FIELD: "v2",
            "filename": "file.json",
        },
        {
            "y": 5,
            INDEX_FIELD: 1,
            REVISION_FIELD: "v2",
            "filename": "file.json",
        },
        {
            "y": 2,
            INDEX_FIELD: 0,
            REVISION_FIELD: "v1",
            "filename": "file.json",
        },
        {
            "y": 3,
            INDEX_FIELD: 1,
            REVISION_FIELD: "v1",
            "filename": "file.json",
        },
    ]
    assert final_props == {"x": INDEX_FIELD, "y": "y", "fields": {"y", "step"}}


def test_to_json_vega(mocker):
    vega_renderer = mocker.MagicMock()
    vega_renderer.TYPE = "vega"
    vega_renderer.get_filled_template.return_value = '{"this": "is vega"}'
    vega_renderer.datapoints = [
        {"x": 1, "y": 2, REVISION_FIELD: "foo", "filename": "foo.json"},
        {"x": 2, "y": 1, REVISION_FIELD: "bar", "filename": "foo.json"},
    ]
    result = to_json(vega_renderer)
    assert result[0] == {
        TYPE_KEY: vega_renderer.TYPE,
        REVISIONS_KEY: ["bar", "foo"],
        "content": {"this": "is vega"},
        "datapoints": {
            "foo": [
                {"x": 1, "y": 2, "filename": "foo.json"},
            ],
            "bar": [
                {"x": 2, "y": 1, "filename": "foo.json"},
            ],
        },
    }
    vega_renderer.get_filled_template.assert_called()


def test_to_json_vega_split(mocker):
    vega_renderer = mocker.MagicMock()
    vega_renderer.TYPE = "vega"
    vega_renderer.get_filled_template.return_value = (
        '{"this": "is split vega"}'
    )
    vega_renderer.datapoints = [
        {"x": 1, "y": 2, REVISION_FIELD: "foo", "filename": "foo.json"},
        {"x": 2, "y": 1, REVISION_FIELD: "bar", "filename": "foo.json"},
    ]
    result = to_json(vega_renderer, split=True)
    assert result[0] == {
        TYPE_KEY: vega_renderer.TYPE,
        REVISIONS_KEY: ["bar", "foo"],
        "content": {"this": "is split vega"},
        "datapoints": {
            "foo": [{"x": 1, "y": 2, "filename": "foo.json"}],
            "bar": [{"x": 2, "y": 1, "filename": "foo.json"}],
        },
    }
    vega_renderer.get_filled_template.assert_called_with(skip_anchors=["data"])


def test_to_json_image(mocker):
    image_renderer = mocker.MagicMock()
    image_renderer.TYPE = "image"
    image_renderer.datapoints = [
        {SRC_FIELD: "contentfoo", REVISION_FIELD: "foo"},
        {SRC_FIELD: "contentbar", REVISION_FIELD: "bar"},
    ]
    result = to_json(image_renderer)
    assert result[0] == {
        "url": image_renderer.datapoints[0].get(SRC_FIELD),
        REVISIONS_KEY: [image_renderer.datapoints[0].get(REVISION_FIELD)],
        TYPE_KEY: image_renderer.TYPE,
    }
