import logging
import os.path
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from funcy import first

from dvc.repo.plots.data import INDEX_FIELD, REVISION_FIELD, to_datapoints
from dvc.utils import relpath

if TYPE_CHECKING:
    from dvc.repo.plots.template import PlotTemplates
    from dvc.types import StrPath

logger = logging.getLogger(__name__)


def get_files(data: Dict) -> Set:
    files = set()
    for rev in data.keys():
        for file in data[rev].get("data", {}).keys():
            files.add(file)
    return files


def group_by_filename(data: Dict) -> List[Dict]:
    # TODO use dpath.util.search once
    #  https://github.com/dpath-maintainers/dpath-python/issues/147 is released
    #  now cannot search when errors are present in data
    files = get_files(data)
    grouped = []

    for file in files:
        tmp: Dict = {}
        for revision, revision_data in data.items():
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

    if found and VegaRenderer.matches(found):
        return VegaRenderer(found, repo.plots.templates).get_vega()
    return ""


def match_renderers(plots_data, templates):
    renderers = []
    for g in group_by_filename(plots_data):
        if VegaRenderer.matches(g):
            renderers.append(VegaRenderer(g, templates))
        if ImageRenderer.matches(g):
            renderers.append(ImageRenderer(g))
    return renderers


def render(repo, plots_data, metrics=None, path=None, html_template_path=None):
    renderers = match_renderers(plots_data, repo.plots.templates)
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


class Renderer:
    def __init__(self, data: Dict):
        self.data = data

        # we assume comparison of same file between revisions
        files = get_files(self.data)
        assert len(files) == 1
        self.filename = files.pop()

    def _convert(self, page_dir_path: "StrPath"):
        raise NotImplementedError

    @property
    def DIV(self):
        raise NotImplementedError

    def generate_html(self, page_dir_path: "StrPath"):
        """this method might edit content of path"""
        partial = self._convert(page_dir_path)
        div_id = f"plot_{self.filename.replace('.', '_').replace('/', '_')}"
        return self.DIV.format(id=div_id, partial=partial)


class VegaRenderer(Renderer):
    DIV = """
    <div id = "{id}">
        <script type = "text/javascript">
            var spec = {partial};
            vegaEmbed('#{id}', spec);
        </script>
    </div>
    """

    def __init__(self, data: Dict, templates: "PlotTemplates"):
        super().__init__(data)
        self.templates = templates

    def _squash_props(self) -> Dict:
        resolved: Dict[str, str] = {}
        for rev_data in self.data.values():
            for file_data in rev_data.get("data", {}).values():
                props = file_data.get("props", {})
                resolved = {**resolved, **props}
        return resolved

    def get_vega(self) -> Optional[str]:
        props = self._squash_props()

        template = self.templates.load(props.get("template") or "default")
        fields = props.get("fields")
        if fields is not None:
            fields = {*fields, props.get("x"), props.get("y")} - {None}

        if not props.get("x") and template.has_anchor("x"):
            props["append_index"] = True
            props["x"] = INDEX_FIELD

        datapoints = []
        for rev, rev_data in self.data.items():
            for file, file_data in rev_data.get("data", {}).items():
                if "data" in file_data:
                    datapoints.extend(
                        to_datapoints(
                            file_data.get("data", []),
                            revision=rev,
                            filename=file,
                            fields=fields,
                            path=props.get("path"),
                            append_index=props.get("append_index", False),
                        )
                    )

        if datapoints:
            if not props.get("y") and template.has_anchor("y"):
                fields = list(first(datapoints))
                skip = (REVISION_FIELD, props.get("x"))
                props["y"] = first(
                    f for f in reversed(fields) if f not in skip
                )
            return template.render(datapoints, props=props)
        return None

    # TODO naming?
    def _convert(self, page_dir_path: "StrPath"):
        return self.get_vega()

    @staticmethod
    def matches(data):
        files = get_files(data)
        extensions = set(map(lambda f: os.path.splitext(f)[1], files))
        return extensions.issubset({".yml", ".yaml", ".json", ".csv", ".tsv"})


class ImageRenderer(Renderer):
    DIV = """
        <div
            id="{id}"
            style="overflow:auto;
                   white-space:nowrap;
                   padding-left:10px;
                   border: 1px solid;">
            {partial}
        </div>"""

    def _write_image(
        self,
        page_dir_path: "StrPath",
        revision: str,
        filename: str,
        image_data: bytes,
    ):
        static = os.path.join(page_dir_path, "static")
        os.makedirs(static, exist_ok=True)

        img_path = os.path.join(
            static, f"{revision}_{filename.replace('/', '_')}"
        )
        rel_img_path = relpath(img_path, page_dir_path)
        with open(img_path, "wb") as fd:
            fd.write(image_data)
        return """
        <div>
            <p>{title}</p>
            <img src="{src}">
        </div>""".format(
            title=revision, src=rel_img_path
        )

    def _convert(self, page_dir_path: "StrPath"):
        div_content = []
        for rev, rev_data in self.data.items():
            if "data" in rev_data:
                for file, file_data in rev_data.get("data", {}).items():
                    if "data" in file_data:
                        div_content.append(
                            self._write_image(
                                page_dir_path, rev, file, file_data["data"]
                            )
                        )
        if div_content:
            div_content.insert(0, f"<p>{self.filename}</p>")
            return "\n".join(div_content)
        return ""

    @staticmethod
    def matches(data):
        files = get_files(data)
        extensions = set(map(lambda f: os.path.splitext(f)[1], files))
        return extensions.issubset({".jpg", ".jpeg", ".gif", ".png"})
