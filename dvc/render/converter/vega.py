from copy import deepcopy
from functools import partial
from typing import Dict, Iterable, List, Optional, Set, Union

from funcy import first, project

from dvc.exceptions import DvcException
from dvc.render import (
    FILENAME_FIELD,
    INDEX_FIELD,
    REVISION_FIELD,
    VERSION_FIELD,
)

from . import Converter


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


def _filter_fields(
    datapoints: List[Dict], fields: Set, **kwargs
) -> List[Dict]:
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


def _find_first_list(
    data: Union[Dict, List], fields: Set, **kwargs
) -> List[Dict]:
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


def _append_index(datapoints: List[Dict], **kwargs) -> List[Dict]:
    if INDEX_FIELD in first(datapoints).keys():
        return datapoints

    for index, data_point in enumerate(datapoints):
        data_point[INDEX_FIELD] = index
    return datapoints


class VegaConverter(Converter):
    """
    Class that takes care of converting unspecified data blob
    (Dict or List[Dict]) into datapoints (List[Dict]).
    If some properties that are required by Template class are missing
    ('x', 'y') it will attempt to fill in the blanks.
    """

    def __init__(self, plot_properties: Optional[Dict] = None):
        super().__init__(plot_properties)
        self.inferred_properties: Dict = {}

        self.steps = []

        self._infer_x()
        self._infer_fields()

        self.steps.append(
            (
                "find_data",
                partial(
                    _find_first_list,
                    fields=self.inferred_properties.get("fields", set())
                    - {INDEX_FIELD},
                ),
            )
        )

        if not self.plot_properties.get("x", None):
            self.steps.append(("append_index", partial(_append_index)))

        self.steps.append(
            (
                "filter_fields",
                partial(
                    _filter_fields,
                    fields=self.inferred_properties.get("fields", set()),
                ),
            )
        )
        self.steps.append(
            (
                "infer_y",
                partial(
                    self._infer_y,
                ),
            )
        )

        self.steps.append(
            (
                "generate_y",
                partial(
                    self._generate_y_values,
                ),
            )
        )

    def _infer_x(self):
        if not self.plot_properties.get("x", None):
            self.inferred_properties["x"] = INDEX_FIELD

    def skip_step(self, name: str):
        self.steps = [(_name, fn) for _name, fn in self.steps if _name != name]

    def _infer_fields(self):
        fields = self.plot_properties.get("fields", set())
        if fields:
            fields = {
                *fields,
                self.plot_properties.get("x", None),
                self.plot_properties.get("y", None),
                self.inferred_properties.get("x", None),
            } - {None}
            self.inferred_properties["fields"] = fields

    def _infer_y(self, datapoints: List[Dict], **kwargs):
        if "y" not in self.plot_properties:
            data_fields = list(first(datapoints))
            skip = (
                REVISION_FIELD,
                self.plot_properties.get("x", None)
                or self.inferred_properties.get("x"),
                FILENAME_FIELD,
                VERSION_FIELD,
            )
            inferred_y = first(
                f for f in reversed(data_fields) if f not in skip
            )
            if "y" in self.inferred_properties:
                previous_y = self.inferred_properties["y"]
                if previous_y != inferred_y:
                    raise DvcException(
                        f"Inferred y ('{inferred_y}' value does not match"
                        f"previously matched one ('f{previous_y}')."
                    )
            else:
                self.inferred_properties["y"] = inferred_y
        return datapoints

    def convert(
        self,
        data,
        revision: str,
        filename: str,
        skip: List = None,
        **kwargs,
    ):
        """
        Convert the data. Fill necessary fields ('x', 'y') and return both
        generated datapoints and updated properties.
        """
        if not skip:
            skip = []

        processed = deepcopy(data)

        for step_name, step in self.steps:
            if step_name not in skip:
                processed = step(  # type: ignore
                    processed,
                    revision=revision,
                    filename=filename,
                )

        return processed, {**self.plot_properties, **self.inferred_properties}

    def _generate_y_values(  # noqa: C901
        self,
        datapoints: List[Dict],
        revision: str,
        filename: str,
        **kwargs,
    ) -> List[Dict]:

        y_values = self.plot_properties.get(
            "y", None
        ) or self.inferred_properties.get("y", None)

        assert y_values is not None

        result = []
        properties_update = {}

        def _add_version_info(datapoint, version_info):
            tmp = datapoint.copy()
            tmp[VERSION_FIELD] = version_info
            return tmp

        def _version_info(revision, filename=None, field=None):
            res = {"revision": revision}
            if filename is not None:
                res["filename"] = filename
            if field is not None:
                res["field"] = field
            return res

        def _generate_y(datapoint, field):
            tmp = datapoint.copy()
            tmp["dvc_inferred_y_value"] = datapoint[field]
            tmp = _add_version_info(
                tmp, _version_info(revision, filename, field)
            )
            if (
                "y_label" not in properties_update
                and "y_label" not in self.plot_properties
            ):
                properties_update["y_label"] = "y"

            properties_update["y"] = "dvc_inferred_y_value"

            return tmp

        if isinstance(y_values, str):
            for datapoint in datapoints:
                result.append(
                    _add_version_info(
                        datapoint, _version_info(revision, filename, y_values)
                    )
                )

        if isinstance(y_values, list):
            for datapoint in datapoints:
                for y_val in y_values:
                    if y_val in datapoint:
                        result.append(_generate_y(datapoint, y_val))

        if isinstance(y_values, dict):

            def _to_set(values: Iterable):
                result = set()
                for val in values:
                    if isinstance(val, list):
                        for elem in val:
                            result.add(elem)
                    else:
                        result.add(val)

                return result

            all_fields = _to_set(y_values.values())
            if (
                all([isinstance(field, str) for field in all_fields])
                and len(all_fields) == 1
            ):
                # if we use the same field from all files,
                # we dont have to generate it
                y_field = all_fields.pop()
                for datapoint in datapoints:
                    result.append(
                        _add_version_info(
                            datapoint,
                            _version_info(revision, filename, y_field),
                        )
                    )
                properties_update.update({"y": y_field})
            else:
                for def_filename, val in y_values.items():
                    if isinstance(val, str):
                        fields = [val]
                    if isinstance(val, list):
                        fields = val
                    for datapoint in datapoints:
                        for field in fields:
                            if field in datapoint and def_filename in filename:
                                result.append(_generate_y(datapoint, field))

        self.inferred_properties = {
            **self.inferred_properties,
            **properties_update,
        }

        return result
