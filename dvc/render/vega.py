import json
import os
from copy import deepcopy
from typing import Dict, List, Optional

from funcy import cached_property

from dvc.render.base import REVISION_FIELD, BadTemplateError, Renderer
from dvc.render.data import Converter, to_datapoints
from dvc.render.utils import get_files
from dvc.repo.plots.template import Template


def _flatten_datapoints(rev_datapoints: Dict[str, List[Dict]]) -> List[Dict]:
    flat = []
    for revision, datapoints in rev_datapoints.items():
        datapoints_cp = deepcopy(datapoints)
        Converter.update(datapoints_cp, {REVISION_FIELD: revision})
        flat.extend(datapoints_cp)
    return flat


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

    # TODO Shouldn't it be a part of plots?
    def _fill_template(self, template, datapoints, props=None, fill_data=True):
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

        if fill_data:
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

    @cached_property
    def _converted_data(self):
        return to_datapoints(self.data, self.properties)

    def _get_filled_template(self, fill_data=True):
        datapoints, final_props = self._converted_data
        flat_datapoints = _flatten_datapoints(datapoints)

        if datapoints:
            filled_template = self._fill_template(
                self.template,
                flat_datapoints,
                final_props,
                fill_data=fill_data,
            )

            return filled_template
        return ""

    # TODO rename
    def asdict(self, fill_data=True):
        filled_template = self._get_filled_template(fill_data)
        if filled_template:
            return json.loads(filled_template)
        return {}

    def as_json(self, **kwargs) -> Optional[str]:

        fill_data = kwargs.get("fill_data", True)

        content = self.asdict(fill_data)
        datapoints, _ = self._converted_data

        return json.dumps(
            [
                {
                    self.TYPE_KEY: self.TYPE,
                    self.REVISIONS_KEY: self._revisions(),
                    "content": content,
                    "datapoints": datapoints,
                }
            ],
        )

    def partial_html(self, **kwargs):
        filled_template = self._get_filled_template()
        return filled_template

    @staticmethod
    def matches(data):
        files = get_files(data)
        extensions = set(map(lambda f: os.path.splitext(f)[1], files))
        return extensions.issubset({".yml", ".yaml", ".json", ".csv", ".tsv"})
