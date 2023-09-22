from typing import Dict, List, Union

from dvc.render import REVISION_FIELD, REVISIONS_KEY, SRC_FIELD, TYPE_KEY
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
        revs = renderer.get_revs()
        if split:
            content, split_content = renderer.get_partial_filled_template()
        else:
            content = renderer.get_filled_template(as_string=False)
            split_content = {}

        return [
            {
                TYPE_KEY: renderer.TYPE,
                REVISIONS_KEY: revs,
                "content": content,
                **split_content,
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
