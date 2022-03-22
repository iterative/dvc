from copy import deepcopy
from functools import partial
from typing import Dict, List, Optional, Set, Union

from funcy import first, project

from dvc.exceptions import DvcException
from dvc.render import FILENAME_FIELD, INDEX_FIELD, REVISION_FIELD


class FieldsNotFoundError(DvcException):
    def __init__(self, expected_fields, found_fields):
        expected_str = ", ".join(expected_fields)
        found_str = ", ".join(found_fields)
        super().__init__(
            f"Could not find all provided fields ('{expected_str}') "
            f"in data fields ('{found_str}')."
        )


class PlotDataStructureError(DvcException):
    def __init__(self):
        super().__init__(
            "Plot data extraction failed. Please see "
            "https://man.dvc.org/plots for supported data formats."
        )


def _filter_fields(datapoints: List[Dict], fields: Set) -> List[Dict]:
    if not fields:
        return datapoints
    assert isinstance(fields, set)

    new_data = []
    for data_point in datapoints:
        keys = set(data_point.keys())
        if not fields <= keys:
            raise FieldsNotFoundError(fields, keys)

        new_data.append(project(data_point, fields))

    return new_data


def _lists(dictionary: Dict):
    for _, value in dictionary.items():
        if isinstance(value, dict):
            yield from _lists(value)
        elif isinstance(value, list):
            yield value


def _find_first_list(data: Union[Dict, List], fields: Set) -> List[Dict]:
    fields = fields or set()

    if not isinstance(data, dict):
        return data

    for lst in _lists(data):
        if (
            all(isinstance(dp, dict) for dp in lst)
            # if fields is empty, it will match any set
            and set(first(lst).keys()) & fields == fields
        ):
            return lst

    raise PlotDataStructureError()


def _append_index(datapoints: List[Dict]) -> List[Dict]:
    if INDEX_FIELD in first(datapoints).keys():
        return datapoints

    for index, data_point in enumerate(datapoints):
        data_point[INDEX_FIELD] = index
    return datapoints


class VegaConverter:
    """
    Class that takes care of converting unspecified data blob
    (Dict or List[Dict]) into datapoints (List[Dict]).
    If some properties that are required by Template class are missing
    ('x', 'y') it will attempt to fill in the blanks.
    """

    def __init__(self, plot_properties: Optional[Dict] = None):
        plot_properties = plot_properties or {}
        self.props = deepcopy(plot_properties)
        self.inferred_props: Dict = {}

        self.steps = []

        self._infer_x()
        self._infer_fields()

        self.steps.append(
            (
                "find_data",
                partial(
                    _find_first_list,
                    fields=self.inferred_props.get("fields", set())
                    - {INDEX_FIELD},
                ),
            )
        )

        if not self.props.get("x", None):
            self.steps.append(("append_index", partial(_append_index)))

        self.steps.append(
            (
                "filter_fields",
                partial(
                    _filter_fields,
                    fields=self.inferred_props.get("fields", set()),
                ),
            )
        )

    def _infer_x(self):
        if not self.props.get("x", None):
            self.inferred_props["x"] = INDEX_FIELD

    def skip_step(self, name: str):
        self.steps = [(_name, fn) for _name, fn in self.steps if _name != name]

    def _infer_fields(self):
        fields = self.props.get("fields", set())
        if fields:
            fields = {
                *fields,
                self.props.get("x", None),
                self.props.get("y", None),
                self.inferred_props.get("x", None),
            } - {None}
            self.inferred_props["fields"] = fields

    def _infer_y(self, datapoints: List[Dict]):
        if "y" not in self.props:
            data_fields = list(first(datapoints))
            skip = (
                REVISION_FIELD,
                self.props.get("x", None) or self.inferred_props.get("x"),
            )
            inferred_y = first(
                f for f in reversed(data_fields) if f not in skip
            )
            if "y" in self.inferred_props:
                previous_y = self.inferred_props["y"]
                if previous_y != inferred_y:
                    raise DvcException(
                        f"Inferred y ('{inferred_y}' value does not match"
                        f"previously matched one ('f{previous_y}')."
                    )
            else:
                self.inferred_props["y"] = inferred_y

    def convert(
        self,
        data: Dict,
        revision: Optional[str] = None,
        filename: Optional[str] = None,
    ):
        """
        Convert the data. Fill necessary fields ('x', 'y') and return both
        generated datapoints and updated properties.
        """
        processed = deepcopy(data)

        for _, step in self.steps:
            processed = step(processed)  # type: ignore

        self._infer_y(processed)  # type: ignore

        if revision:
            for datapoint in processed:
                datapoint[REVISION_FIELD] = revision
        if filename:
            for datapoint in processed:
                datapoint[FILENAME_FIELD] = filename

        return processed, {**self.props, **self.inferred_props}
