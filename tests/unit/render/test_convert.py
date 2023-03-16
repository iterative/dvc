from dvc.render import REVISION_FIELD, REVISIONS_KEY, SRC_FIELD, TYPE_KEY, VERSION_FIELD
from dvc.render.convert import to_json


def test_to_json_vega(mocker):
    vega_renderer = mocker.MagicMock()
    vega_renderer.TYPE = "vega"
    vega_renderer.get_filled_template.return_value = {"this": "is vega"}
    vega_renderer.datapoints = [
        {
            "x": 1,
            "y": 2,
            VERSION_FIELD: {"revision": "foo"},
            "filename": "foo.json",
        },
        {
            "x": 2,
            "y": 1,
            VERSION_FIELD: {"revision": "bar"},
            "filename": "foo.json",
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
                    VERSION_FIELD: {"revision": "foo"},
                },
            ],
            "bar": [
                {
                    "x": 2,
                    "y": 1,
                    "filename": "foo.json",
                    VERSION_FIELD: {"revision": "bar"},
                },
            ],
        },
    }
    vega_renderer.get_filled_template.assert_called()


def test_to_json_vega_split(mocker):
    vega_renderer = mocker.MagicMock()
    vega_renderer.TYPE = "vega"
    vega_renderer.get_filled_template.return_value = {"this": "is split vega"}
    vega_renderer.datapoints = [
        {
            "x": 1,
            "y": 2,
            VERSION_FIELD: {"revision": "foo"},
            "filename": "foo.json",
        },
        {
            "x": 2,
            "y": 1,
            VERSION_FIELD: {"revision": "bar"},
            "filename": "foo.json",
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
                    VERSION_FIELD: {"revision": "foo"},
                }
            ],
            "bar": [
                {
                    "x": 2,
                    "y": 1,
                    "filename": "foo.json",
                    VERSION_FIELD: {"revision": "bar"},
                }
            ],
        },
    }
    vega_renderer.get_filled_template.assert_called_with(
        as_string=False, skip_anchors=["data"]
    )


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
