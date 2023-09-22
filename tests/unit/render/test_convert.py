import json

import pytest

from dvc.render import (
    ANCHOR_DEFINITIONS,
    REVISION_FIELD,
    REVISIONS_KEY,
    SRC_FIELD,
    TYPE_KEY,
)
from dvc.render.convert import to_json


def test_to_json_vega(mocker):
    vega_renderer = mocker.MagicMock()
    vega_renderer.TYPE = "vega"
    vega_renderer.get_revs.return_value = ["bar", "foo"]
    vega_renderer.get_filled_template.return_value = {"this": "is vega"}
    result = to_json(vega_renderer)
    assert result[0] == {
        TYPE_KEY: vega_renderer.TYPE,
        REVISIONS_KEY: ["bar", "foo"],
        "content": {"this": "is vega"},
    }
    vega_renderer.get_filled_template.assert_called()


@pytest.mark.vscode
def test_to_json_vega_split(mocker):
    revs = ["bar", "foo"]
    content = json.dumps(
        {
            "this": "is split vega",
            "encoding": {"color": "<DVC_METRIC_COLOR>"},
            "data": {"values": "<DVC_METRIC_DATA>"},
        }
    )
    anchor_definitions = {
        "<DVC_METRIC_COLOR>": {
            "field": "rev",
            "scale": {
                "domain": revs,
                "range": ["#ff0000", "#00ff00"],
            },
        },
        "<DVC_METRIC_DATA>": [
            {
                "x": 1,
                "y": 2,
                "rev": "foo",
                "filename": "foo.json",
            },
            {
                "x": 2,
                "y": 1,
                "rev": "bar",
                "filename": "foo.json",
            },
        ],
    }

    vega_renderer = mocker.MagicMock()
    vega_renderer.TYPE = "vega"
    vega_renderer.get_partial_filled_template.return_value = (
        content,
        {ANCHOR_DEFINITIONS: anchor_definitions},
    )
    vega_renderer.get_revs.return_value = ["bar", "foo"]

    result = to_json(vega_renderer, split=True)
    assert result[0] == {
        ANCHOR_DEFINITIONS: anchor_definitions,
        TYPE_KEY: vega_renderer.TYPE,
        REVISIONS_KEY: revs,
        "content": content,
    }
    vega_renderer.get_partial_filled_template.assert_called_once()


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
