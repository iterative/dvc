from dvc.render import REVISION_FIELD, REVISIONS_KEY, SRC_FIELD, TYPE_KEY
from dvc.render.convert import to_json


def test_to_json_vega(mocker):
    vega_renderer = mocker.MagicMock()
    vega_renderer.TYPE = "vega"
    vega_renderer.get_filled_template.return_value = '{"this": "is vega"}'
    vega_renderer.datapoints = [
        {
            "x": 1,
            "y": 2,
            "filename": "foo.json",
            REVISION_FIELD: "foo::foo.json::y",
            "dvc_id": "foo::foo.json::y",
            "dvc_rev": "foo",
            "dvc_filename": "foo.json",
            "dvc_field": "y",
        },
        {
            "x": 2,
            "y": 1,
            "filename": "foo.json",
            REVISION_FIELD: "bar::foo.json::y",
            "dvc_id": "bar::foo.json::y",
            "dvc_rev": "bar",
            "dvc_filename": "foo.json",
            "dvc_field": "y",
        },
    ]
    result = to_json(vega_renderer)
    assert result[0] == {
        TYPE_KEY: vega_renderer.TYPE,
        REVISIONS_KEY: ["bar", "foo"],
        "content": {"this": "is vega"},
        "datapoints": {
            "foo": [
                {
                    "x": 1,
                    "y": 2,
                    "filename": "foo.json",
                    REVISION_FIELD: "foo::foo.json::y",
                    "dvc_id": "foo::foo.json::y",
                    "dvc_rev": "foo",
                    "dvc_filename": "foo.json",
                    "dvc_field": "y",
                },
            ],
            "bar": [
                {
                    "x": 2,
                    "y": 1,
                    "filename": "foo.json",
                    REVISION_FIELD: "bar::foo.json::y",
                    "dvc_id": "bar::foo.json::y",
                    "dvc_rev": "bar",
                    "dvc_filename": "foo.json",
                    "dvc_field": "y",
                },
            ],
        },
    }
    vega_renderer.get_filled_template.assert_called()


def test_to_json_vega_split(mocker):
    vega_renderer = mocker.MagicMock()
    vega_renderer.TYPE = "vega"
    vega_renderer.get_filled_template.return_value = '{"this": "is split vega"}'
    vega_renderer.datapoints = [
        {
            "x": 1,
            "y": 2,
            "filename": "foo.json",
            REVISION_FIELD: "foo::foo.json::y",
            "dvc_id": "foo::foo.json::y",
            "dvc_rev": "foo",
            "dvc_filename": "foo.json",
            "dvc_field": "y",
        },
        {
            "x": 2,
            "y": 1,
            "filename": "foo.json",
            REVISION_FIELD: "bar::foo.json::y",
            "dvc_id": "bar::foo.json::y",
            "dvc_rev": "bar",
            "dvc_filename": "foo.json",
            "dvc_field": "y",
        },
    ]
    result = to_json(vega_renderer, split=True)
    assert result[0] == {
        TYPE_KEY: vega_renderer.TYPE,
        REVISIONS_KEY: ["bar", "foo"],
        "content": {"this": "is split vega"},
        "datapoints": {
            "foo": [
                {
                    "x": 1,
                    "y": 2,
                    "filename": "foo.json",
                    REVISION_FIELD: "foo::foo.json::y",
                    "dvc_id": "foo::foo.json::y",
                    "dvc_rev": "foo",
                    "dvc_filename": "foo.json",
                    "dvc_field": "y",
                }
            ],
            "bar": [
                {
                    "x": 2,
                    "y": 1,
                    "filename": "foo.json",
                    REVISION_FIELD: "bar::foo.json::y",
                    "dvc_id": "bar::foo.json::y",
                    "dvc_rev": "bar",
                    "dvc_filename": "foo.json",
                    "dvc_field": "y",
                }
            ],
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
