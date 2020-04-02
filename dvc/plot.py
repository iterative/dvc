import json
import logging
import os

from funcy import cached_property

from dvc.utils.fs import makedirs


logger = logging.getLogger(__name__)


class Template:
    INDENT = 4
    SEPARATORS = (",", ": ")

    def __init__(self, templates_dir):
        self.plot_templates_dir = templates_dir

    def dump(self):
        import json

        makedirs(self.plot_templates_dir, exist_ok=True)

        if not os.path.exists(self.plot_templates_dir):
            makedirs(self.plot_templates_dir)

        with open(
            os.path.join(self.plot_templates_dir, self.TEMPLATE_NAME), "w+"
        ) as fd:
            json.dump(
                self.DEFAULT_CONTENT,
                fd,
                indent=self.INDENT,
                separators=self.SEPARATORS,
            )

    @staticmethod
    def fill(template_path, data, data_src=""):
        assert isinstance(data, list)
        assert all({"x", "y", "revision"} == set(d.keys()) for d in data)

        update_dict = {"data": {"values": data}, "title": data_src}

        with open(template_path, "r") as fd:
            vega_spec = json.load(fd)

        vega_spec.update(update_dict)
        return vega_spec


class DefaultLinearTemplate(Template):
    TEMPLATE_NAME = "default.json"

    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
        "data": {"values": []},
        "mark": {"type": "line"},
        "encoding": {
            "x": {"field": "x", "type": "quantitative"},
            "y": {"field": "y", "type": "quantitative"},
            "color": {"field": "revision", "type": "nominal"},
        },
    }


class DefaultConfusionTemplate(Template):
    TEMPLATE_NAME = "default_confusion.json"
    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
        "data": {"values": []},
        "mark": "rect",
        "encoding": {
            "x": {
                "field": "x",
                "type": "nominal",
                "sort": "ascending",
                "title": "Predicted value",
            },
            "y": {
                "field": "y",
                "type": "nominal",
                "sort": "ascending",
                "title": "Actual value",
            },
            "color": {"aggregate": "count", "type": "quantitative"},
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
