import logging
import os.path

import dpath
from funcy import first

from dvc.repo.plots.data import PlotData, plot_data
from dvc.utils import relpath
from dvc.utils.html import VEGA_DIV_HTML

logger = logging.getLogger(__name__)


def get_files(data):
    files = set()
    for rev in data.keys():
        for file in data[rev].get("data", {}).keys():
            files.add(file)
    return files


def group(data):
    files = get_files(data)
    grouped = []
    for file in files:
        found = dpath.util.search(data, ["*", "*", file])
        if found:
            grouped.append(found)
    return grouped


def find_vega(repo, plots_data, target):
    found = dpath.util.search(plots_data, ["*", "*", target])
    if found and VegaRenderer.matches(found):
        return VegaRenderer(found, repo.plots.templates).get_vega()
    return ""


def prepare_renderers(plots_data, templates, path):
    renderers = []
    for g in group(plots_data):
        if VegaRenderer.matches(g):
            renderers.append(VegaRenderer(g, templates))
        if ImageRenderer.matches(g):
            renderers.append(ImageRenderer(g, document_path=path))
    return renderers


def render(repo, plots_data, metrics=None, path=None, html_template_path=None):
    renderers = prepare_renderers(plots_data, repo.plots.templates, path)
    if not html_template_path:
        html_template_path = repo.config.get("plots", {}).get(
            "html_template", None
        )
        if html_template_path and not os.path.isabs(html_template_path):
            html_template_path = os.path.join(repo.dvc_dir, html_template_path)

    from dvc.utils.html import write

    return write(
        path, renderers, metrics=metrics, template_path=html_template_path
    )


def _resolve_props(data):
    # TODO resolving props from latest
    resolved = None
    for _, rev_data in data.items():
        for _, file_data in rev_data.get("data", {}).items():
            props = file_data.get("props")
            if resolved is None:
                resolved = props
            else:
                resolved = {**resolved, **props}
    return resolved


class VegaRenderer:
    def __init__(self, data, templates=None):
        self.data = data
        self.templates = templates

        files = get_files(self.data)
        assert len(files) == 1
        self.filename = files.pop()

    # TODO RETURN dict?
    def get_vega(self):
        # TODO
        props = _resolve_props(self.data)
        template = self.templates.load(props.get("template") or "default")
        fields = props.get("fields")
        if fields is not None:
            fields = {*fields, props.get("x"), props.get("y")} - {None}

        if not props.get("x") and template.has_anchor("x"):
            props["append_index"] = True
            props["x"] = PlotData.INDEX_FIELD

        datapoints = []
        for rev, rev_data in self.data.items():
            for file, file_data in rev_data.get("data", {}).items():
                if "data" in file_data:
                    datapoints.extend(
                        plot_data(
                            file, rev, file_data.get("data", [])
                        ).to_datapoints(
                            fields=fields,
                            path=props.get("path"),
                            append_index=props.get("append_index", False),
                        )
                    )

        if datapoints:
            if not props.get("y") and template.has_anchor("y"):
                fields = list(first(datapoints))
                skip = (PlotData.REVISION_FIELD, props.get("x"))
                props["y"] = first(
                    f for f in reversed(fields) if f not in skip
                )
            return template.render(datapoints, props=props)
        return None

    # TODO this name
    def get_html(self):
        plot_string = self.get_vega()
        if plot_string:
            html = VEGA_DIV_HTML.format(
                id=f"plot_{self.filename.replace('.', '_').replace('/', '_')}",
                vega_json=plot_string,
            )
            return html
        return None

    @staticmethod
    def matches(data):
        files = get_files(data)
        extensions = set(map(lambda f: os.path.splitext(f)[1], files))
        return extensions.issubset({".yml", ".yaml", ".json", ".csv", ".tsv"})


class ImageRenderer:
    def __init__(self, data, document_path=None):
        self.document_path = document_path
        self.data = data

    def get_html(self):
        static = os.path.join(self.document_path, "static")
        os.makedirs(static, exist_ok=True)
        div = ""
        for rev, rev_data in self.data.items():
            if "data" in rev_data:
                for file, file_data in rev_data.get("data", {}).items():
                    if "data" in file_data:
                        if not div:
                            div += f"<p>{file}</p>"
                        img_path = os.path.join(
                            static, f"{rev}_{file.replace('/', '_')}"
                        )
                        rel_img_path = relpath(img_path, self.document_path)
                        with open(img_path, "wb") as fd:
                            fd.write(file_data["data"])
                        div += (
                            f"<div><p>{rev}</p>"
                            f'<img src="{rel_img_path}"></div>'
                        )
        if div:
            div = (
                '<div style="'
                "overflow:auto;"
                "white-space:nowrap;"
                "padding-left:10px;"
                'border: 1px solid;">'
                f"{div}"
                "</div>"
            )
        return div

    @staticmethod
    def matches(data):
        files = get_files(data)
        extensions = set(map(lambda f: os.path.splitext(f)[1], files))
        return extensions.issubset({".jpg", ".jpeg", ".gif", ".png"})
