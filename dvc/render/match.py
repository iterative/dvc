from collections import defaultdict
from typing import TYPE_CHECKING, Dict, List, Optional

import dpath.options
import dpath.util
from funcy import last

from dvc.repo.plots import infer_data_sources
from dvc.utils.plots import get_plot_id

from .convert import _get_converter

if TYPE_CHECKING:
    from dvc.types import StrPath

dpath.options.ALLOW_EMPTY_STRING_KEYS = True


def _squash_plots_properties(data: List) -> Dict:
    configs = [last(group) for group in data]
    resolved: Dict = {}
    for config in reversed(configs):
        resolved = {**resolved, **config}
    return resolved


class PlotsData:
    def __init__(self, data: Dict):
        self.data = data

    def group_definitions(self):
        groups = defaultdict(list)
        for rev, rev_content in self.data.items():
            for config_file, config_file_content in (
                rev_content.get("definitions", {}).get("data", {}).items()
            ):
                for plot_id, plot_definition in config_file_content.get(
                    "data", {}
                ).items():
                    full_id = get_plot_id(plot_id, config_file)
                    groups[full_id].append((rev, plot_id, plot_definition))
        return dict(groups)

    def get_definition_data(self, target_files, rev):
        result = []
        for file in target_files:
            file_content = (
                self.data.get(rev, {})
                .get("sources", {})
                .get("data", {})
                .get(file, {})
                .get("data", {})
            )
            if file_content:
                result.append((file, file_content))

        return result


def match_defs_renderers(
    data,
    out=None,
    templates_dir: Optional["StrPath"] = None,
):

    from dvc_render import ImageRenderer, VegaRenderer

    plots_data = PlotsData(data)
    renderers = []
    renderer_cls = None
    for plot_id, group in plots_data.group_definitions().items():
        plot_datapoints: List[Dict] = []
        props = _squash_plots_properties(group)
        final_props: Dict = {}

        if out is not None:
            props["out"] = out
        if templates_dir is not None:
            props["template_dir"] = templates_dir

        for rev, inner_id, plot_definition in group:
            plot_sources = infer_data_sources(inner_id, plot_definition)
            definitions_data = plots_data.get_definition_data(
                plot_sources, rev
            )

            if ImageRenderer.matches(inner_id, None):
                renderer_cls = ImageRenderer
                renderer_id = inner_id
            else:
                renderer_cls = VegaRenderer
                renderer_id = plot_id

            converter = _get_converter(renderer_cls, props)

            for filename, plot_data in definitions_data:
                dps, final_props = converter.convert(
                    revision=rev,
                    filename=filename,
                    data=plot_data,
                )
                plot_datapoints.extend(dps)

        if "title" not in final_props:
            final_props["title"] = renderer_id
        if renderer_cls is not None:
            renderers.append(
                renderer_cls(plot_datapoints, renderer_id, **final_props)
            )
    return renderers
