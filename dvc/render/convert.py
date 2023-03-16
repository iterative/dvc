from collections import defaultdict
from typing import Dict, List, Union

from dvc.render import REVISION_FIELD, REVISIONS_KEY, SRC_FIELD, TYPE_KEY, VERSION_FIELD
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


def _group_by_rev(datapoints):
    grouped = defaultdict(list)
    for datapoint in datapoints:
        rev = datapoint.get(VERSION_FIELD, {}).get("revision")
        grouped[rev].append(datapoint)
    return dict(grouped)


def to_json(renderer, split: bool = False) -> List[Dict]:
    if renderer.TYPE == "vega":
        grouped = _group_by_rev(renderer.datapoints)
        if split:
            content = renderer.get_filled_template(
                skip_anchors=["data"], as_string=False
            )
        else:
            content = renderer.get_filled_template(as_string=False)
        if grouped:
            return [
                {
                    TYPE_KEY: renderer.TYPE,
                    REVISIONS_KEY: sorted(grouped.keys()),
                    "content": content,
                    "datapoints": grouped,
                }
            ]
        return []
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
