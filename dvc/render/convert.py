import json
from collections import defaultdict
from typing import Dict, List, Union

from dvc.render import REVISION_FIELD, REVISIONS_KEY, SRC_FIELD, TYPE_KEY
from dvc.render.image_converter import ImageConverter
from dvc.render.vega_converter import VegaConverter


def _get_converter(
    renderer_class, props
) -> Union[VegaConverter, ImageConverter]:
    from dvc_render import ImageRenderer, VegaRenderer

    if renderer_class.TYPE == VegaRenderer.TYPE:
        return VegaConverter(props)
    if renderer_class.TYPE == ImageRenderer.TYPE:
        return ImageConverter(props)

    raise ValueError(f"Invalid renderer class {renderer_class}")


def to_datapoints(renderer_class, data: Dict, props: Dict):
    converter = _get_converter(renderer_class, props)
    datapoints: List[Dict] = []
    for revision, rev_data in data.items():
        for filename, file_data in rev_data.get("data", {}).items():
            if "data" in file_data:
                processed, final_props = converter.convert(
                    file_data.get("data"), revision, filename
                )
                datapoints.extend(processed)
    return datapoints, final_props


def _group_by_rev(datapoints):
    grouped = defaultdict(list)
    for datapoint in datapoints:
        rev = datapoint.pop(REVISION_FIELD)
        grouped[rev].append(datapoint)
    return dict(grouped)


def to_json(renderer, split: bool = False) -> List[Dict]:
    if renderer.TYPE == "vega":
        grouped = _group_by_rev(renderer.datapoints)
        if split:
            content = renderer.get_filled_template(skip_anchors=["data"])
        else:
            content = renderer.get_filled_template()
        return [
            {
                TYPE_KEY: renderer.TYPE,
                REVISIONS_KEY: sorted(grouped.keys()),
                "content": json.loads(content),
                "datapoints": grouped,
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
