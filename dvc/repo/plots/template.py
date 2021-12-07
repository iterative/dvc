import json
import os
from typing import Any, Dict, List, Optional

from funcy import cached_property

from dvc.exceptions import DvcException


class TemplateNotFoundError(DvcException):
    def __init__(self, path):
        super().__init__(f"Template '{path}' not found.")


class NoFieldInDataError(DvcException):
    def __init__(self, field_name):
        super().__init__(
            f"Field '{field_name}' does not exist in provided data."
        )


class TemplateContentDoesNotMatch(DvcException):
    def __init__(self, template_name: str, path: str):
        super().__init__(
            f"Template '{path}' already exists "
            f"and its content is different than '{template_name}' content. "
            "Remove it manually if you want to recreate it."
        )


class Template:
    INDENT = 4
    SEPARATORS = (",", ": ")
    EXTENSION = ".json"
    ANCHOR = "<DVC_METRIC_{}>"

    DEFAULT_CONTENT: Optional[Dict[str, Any]] = None
    DEFAULT_NAME: Optional[str] = None

    def __init__(self, content=None, name=None):
        if content:
            self.content = content
        else:
            self.content = (
                json.dumps(
                    self.DEFAULT_CONTENT,
                    indent=self.INDENT,
                    separators=self.SEPARATORS,
                )
                + "\n"
            )

        self.name = name or self.DEFAULT_NAME
        assert self.content and self.name
        self.filename = self.name + self.EXTENSION

    @classmethod
    def anchor(cls, name):
        return cls.ANCHOR.format(name.upper())

    def has_anchor(self, name):
        return self.anchor_str(name) in self.content

    @classmethod
    def fill_anchor(cls, content, name, value):
        value_str = json.dumps(
            value, indent=cls.INDENT, separators=cls.SEPARATORS, sort_keys=True
        )
        return content.replace(cls.anchor_str(name), value_str)

    @classmethod
    def anchor_str(cls, name):
        return f'"{cls.anchor(name)}"'

    @staticmethod
    def check_field_exists(data, field):
        if not any(field in row for row in data):
            raise NoFieldInDataError(field)


class SimpleLinearTemplate(Template):
    DEFAULT_NAME = "simple"

    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
        "data": {"values": Template.anchor("data")},
        "title": Template.anchor("title"),
        "width": 300,
        "height": 300,
        "mark": {"type": "line"},
        "encoding": {
            "x": {
                "field": Template.anchor("x"),
                "type": "quantitative",
                "title": Template.anchor("x_label"),
            },
            "y": {
                "field": Template.anchor("y"),
                "type": "quantitative",
                "title": Template.anchor("y_label"),
                "scale": {"zero": False},
            },
            "color": {"field": "rev", "type": "nominal"},
        },
    }


class ConfusionTemplate(Template):
    DEFAULT_NAME = "confusion"
    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
        "data": {"values": Template.anchor("data")},
        "title": Template.anchor("title"),
        "facet": {"field": "rev", "type": "nominal"},
        "spec": {
            "transform": [
                {
                    "aggregate": [{"op": "count", "as": "xy_count"}],
                    "groupby": [Template.anchor("y"), Template.anchor("x")],
                },
                {
                    "impute": "xy_count",
                    "groupby": ["rev", Template.anchor("y")],
                    "key": Template.anchor("x"),
                    "value": 0,
                },
                {
                    "impute": "xy_count",
                    "groupby": ["rev", Template.anchor("x")],
                    "key": Template.anchor("y"),
                    "value": 0,
                },
                {
                    "joinaggregate": [
                        {"op": "max", "field": "xy_count", "as": "max_count"}
                    ],
                    "groupby": [],
                },
                {
                    "calculate": "datum.xy_count / datum.max_count",
                    "as": "percent_of_max",
                },
            ],
            "encoding": {
                "x": {
                    "field": Template.anchor("x"),
                    "type": "nominal",
                    "sort": "ascending",
                    "title": Template.anchor("x_label"),
                },
                "y": {
                    "field": Template.anchor("y"),
                    "type": "nominal",
                    "sort": "ascending",
                    "title": Template.anchor("y_label"),
                },
            },
            "layer": [
                {
                    "mark": "rect",
                    "width": 300,
                    "height": 300,
                    "encoding": {
                        "color": {
                            "field": "xy_count",
                            "type": "quantitative",
                            "title": "",
                            "scale": {"domainMin": 0, "nice": True},
                        }
                    },
                },
                {
                    "mark": "text",
                    "encoding": {
                        "text": {"field": "xy_count", "type": "quantitative"},
                        "color": {
                            "condition": {
                                "test": "datum.percent_of_max > 0.5",
                                "value": "white",
                            },
                            "value": "black",
                        },
                    },
                },
            ],
        },
    }


class NormalizedConfusionTemplate(Template):
    DEFAULT_NAME = "confusion_normalized"
    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
        "data": {"values": Template.anchor("data")},
        "title": Template.anchor("title"),
        "facet": {"field": "rev", "type": "nominal"},
        "spec": {
            "transform": [
                {
                    "aggregate": [{"op": "count", "as": "xy_count"}],
                    "groupby": [Template.anchor("y"), Template.anchor("x")],
                },
                {
                    "impute": "xy_count",
                    "groupby": ["rev", Template.anchor("y")],
                    "key": Template.anchor("x"),
                    "value": 0,
                },
                {
                    "impute": "xy_count",
                    "groupby": ["rev", Template.anchor("x")],
                    "key": Template.anchor("y"),
                    "value": 0,
                },
                {
                    "joinaggregate": [
                        {"op": "sum", "field": "xy_count", "as": "sum_y"}
                    ],
                    "groupby": [Template.anchor("y")],
                },
                {
                    "calculate": "datum.xy_count / datum.sum_y",
                    "as": "percent_of_y",
                },
            ],
            "encoding": {
                "x": {
                    "field": Template.anchor("x"),
                    "type": "nominal",
                    "sort": "ascending",
                    "title": Template.anchor("x_label"),
                },
                "y": {
                    "field": Template.anchor("y"),
                    "type": "nominal",
                    "sort": "ascending",
                    "title": Template.anchor("y_label"),
                },
            },
            "layer": [
                {
                    "mark": "rect",
                    "width": 300,
                    "height": 300,
                    "encoding": {
                        "color": {
                            "field": "percent_of_y",
                            "type": "quantitative",
                            "title": "",
                            "scale": {"domain": [0, 1]},
                        }
                    },
                },
                {
                    "mark": "text",
                    "encoding": {
                        "text": {
                            "field": "percent_of_y",
                            "type": "quantitative",
                            "format": ".2f",
                        },
                        "color": {
                            "condition": {
                                "test": "datum.percent_of_y > 0.5",
                                "value": "white",
                            },
                            "value": "black",
                        },
                    },
                },
            ],
        },
    }


class ScatterTemplate(Template):
    DEFAULT_NAME = "scatter"

    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
        "data": {"values": Template.anchor("data")},
        "title": Template.anchor("title"),
        "width": 300,
        "height": 300,
        "layer": [
            {
                "encoding": {
                    "x": {
                        "field": Template.anchor("x"),
                        "type": "quantitative",
                        "title": Template.anchor("x_label"),
                    },
                    "y": {
                        "field": Template.anchor("y"),
                        "type": "quantitative",
                        "title": Template.anchor("y_label"),
                        "scale": {"zero": False},
                    },
                    "color": {"field": "rev", "type": "nominal"},
                },
                "layer": [
                    {"mark": "point"},
                    {
                        "selection": {
                            "label": {
                                "type": "single",
                                "nearest": True,
                                "on": "mouseover",
                                "encodings": ["x"],
                                "empty": "none",
                                "clear": "mouseout",
                            }
                        },
                        "mark": "point",
                        "encoding": {
                            "opacity": {
                                "condition": {
                                    "selection": "label",
                                    "value": 1,
                                },
                                "value": 0,
                            }
                        },
                    },
                ],
            },
            {
                "transform": [{"filter": {"selection": "label"}}],
                "layer": [
                    {
                        "encoding": {
                            "text": {
                                "type": "quantitative",
                                "field": Template.anchor("y"),
                            },
                            "x": {
                                "field": Template.anchor("x"),
                                "type": "quantitative",
                            },
                            "y": {
                                "field": Template.anchor("y"),
                                "type": "quantitative",
                            },
                        },
                        "layer": [
                            {
                                "mark": {
                                    "type": "text",
                                    "align": "left",
                                    "dx": 5,
                                    "dy": -5,
                                },
                                "encoding": {
                                    "color": {
                                        "type": "nominal",
                                        "field": "rev",
                                    }
                                },
                            }
                        ],
                    }
                ],
            },
        ],
    }


class SmoothLinearTemplate(Template):
    DEFAULT_NAME = "smooth"

    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
        "data": {"values": Template.anchor("data")},
        "title": Template.anchor("title"),
        "mark": {"type": "line"},
        "encoding": {
            "x": {
                "field": Template.anchor("x"),
                "type": "quantitative",
                "title": Template.anchor("x_label"),
            },
            "y": {
                "field": Template.anchor("y"),
                "type": "quantitative",
                "title": Template.anchor("y_label"),
                "scale": {"zero": False},
            },
            "color": {"field": "rev", "type": "nominal"},
        },
        "transform": [
            {
                "loess": Template.anchor("y"),
                "on": Template.anchor("x"),
                "groupby": ["rev"],
                "bandwidth": 0.3,
            }
        ],
    }


class LinearTemplate(Template):
    DEFAULT_NAME = "linear"

    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
        "data": {"values": Template.anchor("data")},
        "title": Template.anchor("title"),
        "width": 300,
        "height": 300,
        "layer": [
            {
                "encoding": {
                    "x": {
                        "field": Template.anchor("x"),
                        "type": "quantitative",
                        "title": Template.anchor("x_label"),
                    },
                    "y": {
                        "field": Template.anchor("y"),
                        "type": "quantitative",
                        "title": Template.anchor("y_label"),
                        "scale": {"zero": False},
                    },
                    "color": {"field": "rev", "type": "nominal"},
                },
                "layer": [
                    {"mark": "line"},
                    {
                        "selection": {
                            "label": {
                                "type": "single",
                                "nearest": True,
                                "on": "mouseover",
                                "encodings": ["x"],
                                "empty": "none",
                                "clear": "mouseout",
                            }
                        },
                        "mark": "point",
                        "encoding": {
                            "opacity": {
                                "condition": {
                                    "selection": "label",
                                    "value": 1,
                                },
                                "value": 0,
                            }
                        },
                    },
                ],
            },
            {
                "transform": [{"filter": {"selection": "label"}}],
                "layer": [
                    {
                        "mark": {"type": "rule", "color": "gray"},
                        "encoding": {
                            "x": {
                                "field": Template.anchor("x"),
                                "type": "quantitative",
                            }
                        },
                    },
                    {
                        "encoding": {
                            "text": {
                                "type": "quantitative",
                                "field": Template.anchor("y"),
                            },
                            "x": {
                                "field": Template.anchor("x"),
                                "type": "quantitative",
                            },
                            "y": {
                                "field": Template.anchor("y"),
                                "type": "quantitative",
                            },
                        },
                        "layer": [
                            {
                                "mark": {
                                    "type": "text",
                                    "align": "left",
                                    "dx": 5,
                                    "dy": -5,
                                },
                                "encoding": {
                                    "color": {
                                        "type": "nominal",
                                        "field": "rev",
                                    }
                                },
                            }
                        ],
                    },
                ],
            },
        ],
    }


class PlotTemplates:
    TEMPLATES_DIR = "plots"
    TEMPLATES = [
        SimpleLinearTemplate,
        LinearTemplate,
        ConfusionTemplate,
        NormalizedConfusionTemplate,
        ScatterTemplate,
        SmoothLinearTemplate,
    ]

    @cached_property
    def templates_dir(self):
        return os.path.join(self.dvc_dir, self.TEMPLATES_DIR)

    def get_template(self, template_name: str):
        template_path = os.path.abspath(template_name)
        if os.path.exists(template_path):
            return os.path.abspath(template_path)

        if self.dvc_dir and os.path.exists(self.dvc_dir):
            template_path = os.path.join(self.templates_dir, template_name)
            if os.path.exists(template_path):
                return template_path

            all_templates = [
                os.path.join(root, file)
                for root, _, files in os.walk(self.templates_dir)
                for file in files
            ]
            matches = [
                template
                for template in all_templates
                if os.path.splitext(template)[0] == template_path
            ]
            if matches:
                assert len(matches) == 1
                return matches[0]
        raise TemplateNotFoundError(template_name)

    def __init__(self, dvc_dir):
        self.dvc_dir = dvc_dir

    def init(
        self, output: Optional[str] = None, targets: Optional[List] = None
    ):
        from dvc.utils.fs import makedirs

        output = output or self.templates_dir

        makedirs(output, exist_ok=True)

        if targets:
            templates = [
                template
                for template in self.TEMPLATES
                if template.DEFAULT_NAME in targets
            ]
        else:
            templates = self.TEMPLATES

        for template in templates:
            self._dump(template(), output)

    def _dump(self, template: Template, output: str):
        path = os.path.join(output, template.filename)

        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fd:
                content = fd.read()
            if content != template.content:
                raise TemplateContentDoesNotMatch(
                    template.DEFAULT_NAME or "", path
                )
        else:
            with open(path, "w", encoding="utf-8") as fd:
                fd.write(template.content)

    def load(self, template_name=None):
        if not template_name:
            template_name = "linear"

        try:
            path = self.get_template(template_name)

            with open(path, "r", encoding="utf-8") as fd:
                content = fd.read()

            return Template(content, name=template_name)
        except TemplateNotFoundError:
            for template in self.TEMPLATES:
                if template.DEFAULT_NAME == template_name:
                    return template()
            raise
