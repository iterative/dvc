import os.path
from typing import Dict, List

import dpath.util


def get_files(data: Dict) -> List:
    files = set()
    for rev in data.keys():
        for file in data[rev].get("data", {}).keys():
            files.add(file)
    sorted_files = sorted(files)
    return sorted_files


def group_by_filename(plots_data: Dict) -> List[Dict]:
    files = get_files(plots_data)
    grouped = []
    for file in files:
        grouped.append(dpath.util.search(plots_data, ["*", "*", file]))
    return grouped


def squash_plots_properties(data: Dict) -> Dict:
    resolved: Dict[str, str] = {}
    for rev_data in data.values():
        for file_data in rev_data.get("data", {}).values():
            props = file_data.get("props", {})
            resolved = {**resolved, **props}
    return resolved


def match_renderers(plots_data, templates):
    from dvc.render import RENDERERS

    renderers = []
    for group in group_by_filename(plots_data):

        plot_properties = squash_plots_properties(group)
        template = templates.load(plot_properties.get("template", None))

        for renderer_class in RENDERERS:
            if renderer_class.matches(group):
                renderers.append(
                    renderer_class(
                        group, template=template, properties=plot_properties
                    )
                )
    return renderers


def render(
    repo,
    renderers,
    metrics=None,
    path=None,
    html_template_path=None,
    refresh_seconds=None,
):
    if not html_template_path:
        html_template_path = repo.config.get("plots", {}).get(
            "html_template", None
        )
        if html_template_path and not os.path.isabs(html_template_path):
            html_template_path = os.path.join(repo.dvc_dir, html_template_path)

    from dvc.render.html import write

    return write(
        path,
        renderers,
        metrics=metrics,
        template_path=html_template_path,
        refresh_seconds=refresh_seconds,
    )
