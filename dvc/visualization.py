import json
import logging
import os
import tempfile

from dvc.utils.fs import makedirs


logger = logging.getLogger(__name__)


class AbstractTemplate:
    HTML_TEMPLATE = """<!DOCTYPE html>
<html>
  <head>
    <title>Embedding Vega-Lite</title>
    <script src="https://cdn.jsdelivr.net/npm/vega@5.10.0"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@4.8.1"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6.5.1"></script>
  </head>
  <body>
    <div id="vis"></div>

    <script type="text/javascript">
      var yourVlSpec = {vega_json};
      vegaEmbed('#vis', yourVlSpec);
    </script>
  </body>
</html>"""

    TEMPLATES_DIR = "visualisation"
    INDENT = 4
    SEPARATORS = (",", ": ")

    def __init__(self, dvc_dir):
        self.dvc_dir = dvc_dir
        self.visualization_dir = os.path.join(dvc_dir, self.TEMPLATES_DIR)

    def dump(self):
        import json

        makedirs(self.visualization_dir, exist_ok=True)

        with open(
            os.path.join(self.visualization_dir, self.TEMPLATE_NAME), "w+"
        ) as fd:
            json.dump(
                self.DEFAULT_CONTENT,
                fd,
                indent=self.INDENT,
                separators=self.SEPARATORS,
            )

    def load(self):
        import json

        with open(
            os.path.join(self.visualization_dir, self.TEMPLATE_NAME), "r"
        ) as fd:
            return json.load(fd)

    def fill(self, data):
        raise NotImplementedError

    def save(self, data):

        vega_json = self.fill(data)

        tmp_dir = tempfile.mkdtemp("dvc-viz")
        path = os.path.join(tmp_dir, "vis.html")
        with open(path, "w") as fd:
            fd.write(self.HTML_TEMPLATE.format(vega_json=vega_json))

        logger.error("PATH: {}".format(path))


class Default1DArrayTemplate(AbstractTemplate):
    def fill(self, data):
        assert isinstance(data, list)
        assert not isinstance(data[0], list)
        with open(
            os.path.join(self.visualization_dir, self.TEMPLATE_NAME), "r"
        ) as fd:
            content = json.load(fd)

        data_entry_template = '{{"x":{},"y":{}}},'
        to_inject = "["
        for index, v in enumerate(data):
            to_inject += data_entry_template.format(index, v)
        to_inject += "]"

        content["data"][0]["values"] = to_inject
        return str(content)

    TEMPLATE_NAME = "default_1d_array.json"
    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "width": 500,
        "height": 500,
        "data": [{"name": "table", "values": ""}],
        "scales": [
            {
                "name": "x",
                "type": "point",
                "range": "width",
                "domain": {"data": "table", "field": "x"},
            },
            {
                "name": "y",
                "type": "linear",
                "range": "height",
                "domain": {"data": "table", "field": "y"},
            },
        ],
        "axes": [
            {"orient": "bottom", "scale": "x"},
            {"orient": "left", "scale": "y"},
        ],
        "marks": [
            {
                "type": "line",
                "from": {"data": "table"},
                "encode": {
                    "enter": {
                        "x": {"scale": "x", "field": "x"},
                        "y": {"scale": "y", "field": "y"},
                        "strokeWidth": {"value": 3},
                    }
                },
            }
        ],
    }


TEMPLATES = [Default1DArrayTemplate]


class VisualizationTemplates:
    @staticmethod
    def init(dvc_dir):
        [t(dvc_dir).dump() for t in TEMPLATES]
