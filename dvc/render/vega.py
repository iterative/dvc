import os
from copy import copy, deepcopy
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from funcy import first

from dvc.exceptions import DvcException
from dvc.render import Renderer
from dvc.render.utils import get_files

if TYPE_CHECKING:
    from dvc.repo.plots.template import PlotTemplates
    from dvc.types import StrPath

REVISION_FIELD = "rev"
INDEX_FIELD = "step"


class PlotMetricTypeError(DvcException):
    def __init__(self, file):
        super().__init__(
            "'{}' - file type error\n"
            "Only JSON, YAML, CSV and TSV formats are supported.".format(file)
        )


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

    def get_vega(self) -> Optional[str]:
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
            return template.render(datapoints, props=props)
        return None

    def _convert(self, path: "StrPath"):
        return self.get_vega()

    @staticmethod
    def matches(data):
        files = get_files(data)
        extensions = set(map(lambda f: os.path.splitext(f)[1], files))
        return extensions.issubset({".yml", ".yaml", ".json", ".csv", ".tsv"})
