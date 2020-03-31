import json
import logging
import os

from dvc.utils.fs import makedirs


logger = logging.getLogger(__name__)


class AbstractTemplate:

    TEMPLATES_DIR = "plot"
    INDENT = 4
    SEPARATORS = (",", ": ")

    def __init__(self, dvc_dir):
        self.dvc_dir = dvc_dir
        self.plot_templates_dir = os.path.join(dvc_dir, self.TEMPLATES_DIR)

    def dump(self):
        import json

        makedirs(self.plot_templates_dir, exist_ok=True)

        with open(
            os.path.join(self.plot_templates_dir, self.TEMPLATE_NAME), "w+"
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
            os.path.join(self.plot_templates_dir, self.TEMPLATE_NAME), "r"
        ) as fd:
            return json.load(fd)

    def fill(self, data):
        raise NotImplementedError

    # def save(self, update_dict, path):
    #     vega_dict = self.fill(update_dict)

    # with open(path, "w") as fd:
    #     fd.write(self.HTML_TEMPLATE.format(vega_json=vega_dict))

    # logger.error("PATH: {}".format(path))


class DefaultTemplate(AbstractTemplate):
    TEMPLATE_NAME = "default.json"

    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
        "title": "",
        "data": {"values": []},
        "mark": {"type": "line"},
        "encoding": {
            "x": {"field": "x", "type": "quantitative"},
            "y": {"field": "y", "type": "quantitative"},
            "color": {"field": "revision", "type": "nominal"},
        },
    }

    def fill(self, update_dict):
        with open(
            os.path.join(self.plot_templates_dir, self.TEMPLATE_NAME), "r"
        ) as fd:
            vega_spec = json.load(fd)

        vega_spec.update(update_dict)
        return vega_spec


TEMPLATES = [DefaultTemplate]


class PlotTemplates:
    @staticmethod
    def init(dvc_dir):
        [t(dvc_dir).dump() for t in TEMPLATES]
