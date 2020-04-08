import json
import logging
import os
import random
import re
import string

from funcy import cached_property

from dvc.exceptions import DvcException
from dvc.utils.fs import makedirs


logger = logging.getLogger(__name__)

PAGE_HTML = """<html>
<head>
    <title>dvc plot</title>
    <script src="https://cdn.jsdelivr.net/npm/vega@5.10.0"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@4.8.1"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6.5.1"></script>
</head>
<body>
    {divs}
</body>
</html>"""

DIV_HTML = """<div id = "{id}"></div>
<script type = "text/javascript">
    var spec = {vega_json};
    vegaEmbed('#{id}', spec);
</script>"""


def _save_plot_html(divs, path):
    page = PAGE_HTML.format(divs="\n".join(divs))
    with open(path, "w") as fobj:
        fobj.write(page)


def _prepare_div(vega_dict):
    id = "".join(random.sample(string.ascii_lowercase, 8))
    return DIV_HTML.format(
        id=str(id),
        vega_json=json.dumps(vega_dict, indent=4, separators=(",", ": ")),
    )


class Template:
    INDENT = 4
    SEPARATORS = (",", ": ")
    EXTENTION = ".dvct"
    METRIC_DATA_STRING = "<DVC_METRIC_DATA>"

    def __init__(self, templates_dir):
        self.plot_templates_dir = templates_dir

    def dump(self):
        makedirs(self.plot_templates_dir, exist_ok=True)

        if not os.path.exists(self.plot_templates_dir):
            makedirs(self.plot_templates_dir)

        div = _prepare_div(self.DEFAULT_CONTENT)

        _save_plot_html(
            [div],
            os.path.join(
                self.plot_templates_dir, self.TEMPLATE_NAME + self.EXTENTION
            ),
        )
        # json.dump(
        #         self.DEFAULT_CONTENT,
        #         fd,
        #         indent=self.INDENT,
        #         separators=self.SEPARATORS,
        #     )

    def load_template(self, path):
        try:
            with open(path, "r") as fd:
                return json.load(fd)
        except FileNotFoundError:
            try:
                with open(
                    os.path.join(self.plot_templates_dir, path), "r"
                ) as fd:
                    return json.load(fd)
            except FileNotFoundError:
                raise DvcException("Not in repo nor in defaults")

    @staticmethod
    def parse_data_placeholders(template_path):
        regex = re.compile("<DVC_METRIC_DATA.*>")
        with open(template_path, "r") as fobj:
            template_content = fobj.read()
        matches = regex.findall(template_content)
        data_files = [
            m.replace("<", "")
            .replace(">", "")
            .replace("DVC_METRIC_DATA::", "")
            for m in matches
        ]
        return data_files

    @staticmethod
    def fill(template_path, data):
        with open(template_path, "r") as fobj:
            template_str = fobj.read()
        regex = re.compile('"<DVC_METRIC_DATA.*>"')
        matches = regex.findall(template_str)

        result_path = os.path.basename(template_path).replace(".dvct", ".html")
        result_content = template_str.replace(
            matches[0],
            json.dumps(
                data, indent=Template.INDENT, separators=Template.SEPARATORS
            ),
        )
        with open(result_path, "w") as fobj:
            fobj.write(result_content)
        return result_path


class DefaultLinearTemplate(Template):
    TEMPLATE_NAME = "default"

    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
        "data": {"values": Template.METRIC_DATA_STRING},
        "mark": {"type": "line"},
        "encoding": {
            "x": {"field": "x", "type": "quantitative"},
            "y": {"field": "y", "type": "quantitative"},
            "color": {"field": "revision", "type": "nominal"},
        },
    }


class DefaultConfusionTemplate(Template):
    TEMPLATE_NAME = "cf"
    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
        "data": {"values": Template.METRIC_DATA_STRING},
        "mark": "rect",
        "encoding": {
            "x": {
                "field": "predicted",
                "type": "nominal",
                "sort": "ascending",
            },
            "y": {"field": "actual", "type": "nominal", "sort": "ascending"},
            "color": {"aggregate": "count", "type": "quantitative"},
            "facet": {"field": "revision", "type": "nominal"},
        },
    }


class PlotTemplates:
    TEMPLATES_DIR = "plot"
    TEMPLATES = [DefaultLinearTemplate, DefaultConfusionTemplate]

    @cached_property
    def templates_dir(self):
        return os.path.join(self.dvc_dir, self.TEMPLATES_DIR)

    def __init__(self, dvc_dir):
        self.dvc_dir = dvc_dir

        if not os.path.exists(self.templates_dir):
            makedirs(self.templates_dir, exist_ok=True)
            for t in self.TEMPLATES:
                t(self.templates_dir).dump()
