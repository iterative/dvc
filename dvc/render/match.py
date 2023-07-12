import logging
import os
from collections import defaultdict
from typing import TYPE_CHECKING, DefaultDict, Dict, List, NamedTuple, Optional

import dpath
import dpath.options
from funcy import get_in, last

from dvc.repo.plots import _normpath, infer_data_sources
from dvc.utils.plots import get_plot_id

from .convert import _get_converter

if TYPE_CHECKING:
    from dvc.types import StrPath
    from dvc_render.base import Renderer


dpath.options.ALLOW_EMPTY_STRING_KEYS = True
logger = logging.getLogger(__name__)


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
        result = {}
        for definition_file in target_files:
            if os.name == "nt":
                source_file = _normpath(definition_file).replace("\\", "/")
            else:
                source_file = definition_file
            file_content = (
                self.data.get(rev, {})
                .get("sources", {})
                .get("data", {})
                .get(source_file, {})
                .get("data", {})
            )
            if file_content:
                result[definition_file] = file_content
        return result


class RendererWithErrors(NamedTuple):
    renderer: "Renderer"
    source_errors: Dict[str, Dict[str, Exception]]
    definition_errors: Dict[str, Exception]


def match_defs_renderers(  # noqa: C901, PLR0912
    data,
    out=None,
    templates_dir: Optional["StrPath"] = None,
) -> List[RendererWithErrors]:
    from dvc_render import ImageRenderer, VegaRenderer

    plots_data = PlotsData(data)
    renderers = []
    renderer_cls = None

    for plot_id, group in plots_data.group_definitions().items():
        plot_datapoints: List[Dict] = []
        props = _squash_plots_properties(group)
        final_props: Dict = {}

        def_errors: Dict[str, Exception] = {}
        src_errors: DefaultDict[str, Dict[str, Exception]] = defaultdict(dict)

        if out is not None:
            props["out"] = out
        if templates_dir is not None:
            props["template_dir"] = templates_dir

        for rev, inner_id, plot_definition in group:
            plot_sources = infer_data_sources(inner_id, plot_definition)
            definitions_data = plots_data.get_definition_data(plot_sources, rev)

            if ImageRenderer.matches(inner_id, None):
                renderer_cls = ImageRenderer
                renderer_id = inner_id
            else:
                renderer_cls = VegaRenderer
                renderer_id = plot_id

            converter = _get_converter(renderer_cls, inner_id, props, definitions_data)

            for src in plot_sources:
                if error := get_in(data, [rev, "sources", "data", src, "error"]):
                    src_errors[rev][src] = error

            try:
                dps, rev_props = converter.flat_datapoints(rev)
            except Exception as e:  # noqa: BLE001, pylint: disable=broad-except
                logger.warning("In %r, %s", rev, str(e).lower())
                def_errors[rev] = e
                continue

            if not final_props and rev_props:
                final_props = rev_props
            plot_datapoints.extend(dps)

        if "title" not in final_props:
            final_props["title"] = renderer_id

        if renderer_cls is not None:
            renderer = renderer_cls(plot_datapoints, renderer_id, **final_props)
            renderers.append(RendererWithErrors(renderer, dict(src_errors), def_errors))
    return renderers
