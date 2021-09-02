import os.path
from typing import Dict, List, Set


def get_files(data: Dict) -> Set:
    files = set()
    for rev in data.keys():
        for file in data[rev].get("data", {}).keys():
            files.add(file)
    return files


def group_by_filename(plots_data: Dict) -> List[Dict]:
    # TODO use dpath.util.search once
    #  https://github.com/dpath-maintainers/dpath-python/issues/147 is released
    #  now cannot search when errors are present in data
    files = get_files(plots_data)
    grouped = []

    for file in files:
        tmp: Dict = {}
        for revision, revision_data in plots_data.items():
            if file in revision_data.get("data", {}):
                if "data" not in tmp:
                    tmp[revision] = {"data": {}}
                tmp[revision]["data"].update(
                    {file: revision_data["data"][file]}
                )
        grouped.append(tmp)

    return grouped


def find_vega(repo, plots_data, target):
    # TODO same as group_by_filename
    grouped = group_by_filename(plots_data)
    found = None
    for plot_group in grouped:
        files = get_files(plot_group)
        assert len(files) == 1
        file = files.pop()
        if file == target:
            found = plot_group
            break

    from dvc.render.vega import VegaRenderer

    if found and VegaRenderer.matches(found):
        return VegaRenderer(found, repo.plots.templates).get_vega()
    return ""


def match_renderers(plots_data, templates):
    from dvc.render.image import ImageRenderer
    from dvc.render.vega import VegaRenderer

    renderers = []
    for g in group_by_filename(plots_data):
        if VegaRenderer.matches(g):
            renderers.append(VegaRenderer(g, templates))
        if ImageRenderer.matches(g):
            renderers.append(ImageRenderer(g))
    return renderers


def render(
    repo,
    plots_data,
    metrics=None,
    path=None,
    html_template_path=None,
    refresh_seconds=None,
):
    # TODO we could probably remove repo usages (here and in VegaRenderer)
    renderers = match_renderers(plots_data, repo.plots.templates)
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
