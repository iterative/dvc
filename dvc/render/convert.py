from typing import Dict, List, Union

from dvc.render import (
    ANCHORS_Y_DEFN,
    REVISION_FIELD,
    REVISIONS_KEY,
    SRC_FIELD,
    TYPE_KEY,
)
from dvc.render.converter.image import ImageConverter
from dvc.render.converter.vega import VegaConverter


def _get_converter(
    renderer_class, renderer_id, props, data
) -> Union[VegaConverter, ImageConverter]:
    from dvc_render import ImageRenderer, VegaRenderer

    if renderer_class.TYPE == VegaRenderer.TYPE:
        return VegaConverter(renderer_id, data, props)
    if renderer_class.TYPE == ImageRenderer.TYPE:
        return ImageConverter(renderer_id, data, props)

    raise ValueError(f"Invalid renderer class {renderer_class}")


def to_json(renderer, split: bool = False) -> List[Dict]:
    if renderer.TYPE == "vega":
        if not renderer.datapoints:
            return []
        if split:
            content = renderer.get_filled_template(
                skip_anchors=["data"], as_string=False
            )
        else:
            content = renderer.get_filled_template(as_string=False)

        return [
            {
                ANCHORS_Y_DEFN: renderer.properties.get("anchors_y_defn", {}),
                TYPE_KEY: renderer.TYPE,
                REVISIONS_KEY: renderer.properties.get("anchor_revs", []),
                "content": content,
                "datapoints": renderer.datapoints,
            }
        ]
    if renderer.TYPE == "image":
        return [
            {
                TYPE_KEY: renderer.TYPE,
                REVISIONS_KEY: [datapoint.get(REVISION_FIELD)],
                "url": datapoint.get(SRC_FIELD),
            }
            for datapoint in renderer.datapoints
        ]
    raise ValueError(f"Invalid renderer: {renderer.TYPE}")
