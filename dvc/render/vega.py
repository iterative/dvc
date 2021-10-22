import json
import os
from copy import copy, deepcopy
from typing import Dict, List, Optional, Union

from funcy import first

from dvc.exceptions import DvcException
from dvc.render.base import (
    INDEX_FIELD,
    REVISION_FIELD,
    BadTemplateError,
    Renderer,
)
from dvc.render.utils import get_files


class PlotDataStructureError(DvcException):
    def __init__(self):
        super().__init__(
            "Plot data extraction failed. Please see "
            "https://man.dvc.org/plots for supported data formats."
        )


def _filter_fields(
    datapoints: List[Dict], filename, revision, fields=None
) -> List[Dict]:
    if not fields:
        return datapoints
    assert isinstance(fields, set)

    new_data = []
    for data_point in datapoints:
        new_dp = copy(data_point)

        keys = set(data_point.keys())
        if keys & fields != fields:
            raise DvcException(
                "Could not find fields: '{}' for '{}' at '{}'.".format(
                    ", ".join(fields), filename, revision
                )
            )

        to_del = keys - fields
        for key in to_del:
            del new_dp[key]
        new_data.append(new_dp)
    return new_data


def _lists(dictionary):
    for _, value in dictionary.items():
        if isinstance(value, dict):
            yield from _lists(value)
        elif isinstance(value, list):
            yield value


def _find_data(data: Union[Dict, List], fields=None) -> List[Dict]:
    if not isinstance(data, dict):
        return data

    if not fields:
        # just look for first list of dicts
        fields = set()

    for lst in _lists(data):
        if (
            all(isinstance(dp, dict) for dp in lst)
            and set(first(lst).keys()) & fields == fields
        ):
            return lst
    raise PlotDataStructureError()


def _append_index(datapoints: List[Dict], append_index=False) -> List[Dict]:
    if not append_index or INDEX_FIELD in first(datapoints).keys():
        return datapoints

    for index, data_point in enumerate(datapoints):
        data_point[INDEX_FIELD] = index
    return datapoints


def _append_revision(datapoints: List[Dict], revision) -> List[Dict]:
    for data_point in datapoints:
        data_point[REVISION_FIELD] = revision
    return datapoints


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

    def _squash_props(self) -> Dict:
        resolved: Dict[str, str] = {}
        for rev_data in self.data.values():
            for file_data in rev_data.get("data", {}).values():
                props = file_data.get("props", {})
                resolved = {**resolved, **props}
        return resolved

    def _revisions(self):
        return list(self.data.keys())

    def _datapoints(self, props: Dict):
        fields = props.get("fields", set())
        if fields:
            fields = {*fields, props.get("x"), props.get("y")} - {None}

        datapoints = []
        for revision, rev_data in self.data.items():
            for filename, file_data in rev_data.get("data", {}).items():
                if "data" in file_data:
                    tmp = deepcopy(file_data.get("data"))
                    tmp = _find_data(tmp, fields=fields - {INDEX_FIELD})
                    tmp = _append_index(
                        tmp, append_index=props.get("append_index", False)
                    )
                    tmp = _filter_fields(
                        tmp,
                        filename=filename,
                        revision=revision,
                        fields=fields,
                    )
                    tmp = _append_revision(tmp, revision=revision)
                    datapoints.extend(tmp)
        return datapoints

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
        props = self._squash_props()

        template = self.templates.load(props.get("template", None))

        if not props.get("x") and template.has_anchor("x"):
            props["append_index"] = True
            props["x"] = INDEX_FIELD

        datapoints = self._datapoints(props)

        if datapoints:
            if not props.get("y") and template.has_anchor("y"):
                fields = list(first(datapoints))
                skip = (REVISION_FIELD, props.get("x"))
                props["y"] = first(
                    f for f in reversed(fields) if f not in skip
                )
            filled_template = self._fill_template(template, datapoints, props)

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
