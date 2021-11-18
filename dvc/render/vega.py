import json
import os
from copy import deepcopy
from typing import Dict, Optional

from dvc.render.base import BadTemplateError, Renderer
from dvc.render.data import to_datapoints
from dvc.render.utils import get_files
from dvc.repo.plots.template import Template


class VegaRenderer(Renderer):
    TYPE = "vega"

    DIV = """
    <div id = "{id}">
        <script type = "text/javascript">
            var spec = {partial};
            vegaEmbed('#{id}', spec);
        </script>
    </div>
    """

    SCRIPTS = """
    <script src="https://cdn.jsdelivr.net/npm/vega@5.20.2"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@5.1.0"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6.18.2"></script>
    """

    def __init__(
        self, data: Dict, template: Template, properties: Dict = None
    ):
        super().__init__(data)
        self.properties = properties or {}
        self.template = template

    def _revisions(self):
        return list(self.data.keys())

    def _fill_template(self, template, datapoints, props=None):
        props = props or {}

        content = deepcopy(template.content)
        if template.anchor_str("data") not in template.content:
            anchor = template.anchor("data")
            raise BadTemplateError(
                f"Template '{template.name}' is not using '{anchor}' anchor"
            )

        if props.get("x"):
            template.check_field_exists(datapoints, props.get("x"))
        if props.get("y"):
            template.check_field_exists(datapoints, props.get("y"))

        content = template.fill_anchor(content, "data", datapoints)

        props.setdefault("title", "")
        props.setdefault("x_label", props.get("x"))
        props.setdefault("y_label", props.get("y"))

        names = ["title", "x", "y", "x_label", "y_label"]
        for name in names:
            value = props.get(name)
            if value is not None:
                content = template.fill_anchor(content, name, value)

        return content

    def get_filled_template(self):
        props = self.properties
        datapoints, final_props = to_datapoints(self.data, props)

        if datapoints:
            filled_template = self._fill_template(
                self.template, datapoints, final_props
            )

            return filled_template
        return None

    def asdict(self):
        filled_template = self.get_filled_template()
        if filled_template:
            return json.loads(filled_template)
        return {}

    def as_json(self, **kwargs) -> Optional[str]:

        content = self.asdict()

        return json.dumps(
            [
                {
                    self.TYPE_KEY: self.TYPE,
                    self.REVISIONS_KEY: self._revisions(),
                    "content": content,
                }
            ],
        )

    def partial_html(self, **kwargs):
        return self.get_filled_template() or ""

    @staticmethod
    def matches(data):
        files = get_files(data)
        extensions = set(map(lambda f: os.path.splitext(f)[1], files))
        return extensions.issubset({".yml", ".yaml", ".json", ".csv", ".tsv"})
