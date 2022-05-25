from typing import TYPE_CHECKING, Dict, Optional

import dpath.util

from dvc_render import RENDERERS

from .convert import to_datapoints

if TYPE_CHECKING:
    from dvc.types import StrPath


def group_by_filename(plots_data: Dict) -> Dict:
    grouped: Dict[str, Dict] = {}

    for revision in plots_data.keys():
        data = plots_data[revision].get("data", {})
        for file in data.keys():
            content = data.get(file)
            if content:
                dpath.util.new(
                    grouped, [file, revision, "data", file], content
                )

    return grouped


def squash_plots_properties(data: Dict) -> Dict:
    resolved: Dict[str, str] = {}
    for rev_data in data.values():
        for file_data in rev_data.get("data", {}).values():
            props = file_data.get("props", {})
            resolved = {**resolved, **props}
    return resolved


def match_renderers(
    plots_data,
    out: Optional["StrPath"] = None,
    templates_dir: Optional["StrPath"] = None,
):
    renderers = []
    for filename, group in group_by_filename(plots_data).items():
        plot_properties = squash_plots_properties(group)
        for renderer_class in RENDERERS:
            if renderer_class.matches(filename, plot_properties):
                if out is not None:
                    plot_properties["out"] = out
                if templates_dir is not None:
                    plot_properties["template_dir"] = templates_dir
                datapoints, plot_properties = to_datapoints(
                    renderer_class, group, plot_properties
                )
                renderers.append(
                    renderer_class(datapoints, filename, **plot_properties)
                )
                break
    return renderers
