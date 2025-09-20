import os
from collections.abc import Iterable
from typing import Any, Optional, Union

from funcy import first, last

from dvc.exceptions import DvcException
from dvc.render import FIELD, FILENAME, INDEX, REVISION

from . import Converter


class FieldNotFoundError(DvcException):
    def __init__(self, expected_field, found_fields):
        found_str = ", ".join(found_fields)
        super().__init__(
            f"Could not find provided field ('{expected_field}') "
            f"in data fields ('{found_str}')."
        )


def _lists(blob: Union[dict, list]) -> Iterable[list]:
    if isinstance(blob, list):
        yield blob
    else:
        for value in blob.values():
            if isinstance(value, dict):
                yield from _lists(value)
            elif isinstance(value, list):
                yield value


def _file_field(*args):
    for axis_def in args:
        if axis_def is not None:
            for file, val in axis_def.items():
                if isinstance(val, str):
                    yield file, val
                elif isinstance(val, list):
                    for field in val:
                        yield file, field


def _find(filename: str, field: str, data_series: list[tuple[str, str, Any]]):
    for data_file, data_field, data in data_series:
        if data_file == filename and data_field == field:
            return data_file, data_field, data
    return None


def _verify_field(file2datapoints: dict[str, list], filename: str, field: str):
    if filename in file2datapoints:
        datapoint = first(file2datapoints[filename])
        if field not in datapoint:
            raise FieldNotFoundError(field, datapoint.keys())


def _get_xs(properties: dict, file2datapoints: dict[str, list[dict]]):
    x = properties.get("x")
    if x is not None and isinstance(x, dict):
        for filename, field in _file_field(x):
            _verify_field(file2datapoints, filename, field)
            yield filename, field


def _get_ys(properties, file2datapoints: dict[str, list[dict]]):
    y = properties.get("y", None)
    if y is not None:
        for filename, field in _file_field(y):
            _verify_field(file2datapoints, filename, field)
            yield filename, field


def _is_datapoints(lst: list[dict]):
    """
    check if dict keys match, datapoints with different keys mgiht lead
    to unexpected behavior
    """

    return all(isinstance(item, dict) for item in lst) and set(first(lst).keys()) == {
        key for keys in lst for key in keys
    }


def get_datapoints(file_content: dict):
    result: list[dict[str, Any]] = []
    for lst in _lists(file_content):
        if _is_datapoints(lst):
            for index, datapoint in enumerate(lst):
                if len(result) <= index:
                    result.append({})
                result[index].update(datapoint)
    return result


class VegaConverter(Converter):
    """
    Class that takes care of converting unspecified data blob
    (Dict or List[Dict]) into datapoints (List[Dict]).
    If some properties that are required by Template class are missing
    ('x', 'y') it will attempt to fill in the blanks.
    """

    def __init__(
        self,
        plot_id: str,
        data: Optional[dict] = None,
        properties: Optional[dict] = None,
    ):
        super().__init__(plot_id, data, properties)
        self.plot_id = plot_id

    def _infer_y_from_data(self):
        if self.plot_id in self.data:
            for lst in _lists(self.data[self.plot_id]):
                if all(isinstance(item, dict) for item in lst):
                    datapoint = first(lst)
                    field = last(datapoint.keys())
                    return {self.plot_id: field}
        return None

    def _infer_x_y(self):
        x = self.properties.get("x", None)
        y = self.properties.get("y", None)

        inferred_properties: dict = {}

        # Infer x.
        if isinstance(x, str):
            inferred_properties["x"] = {}
            # If multiple y files, duplicate x for each file.
            if isinstance(y, dict):
                for file, fields in y.items():
                    # Duplicate x for each y.
                    if isinstance(fields, list):
                        inferred_properties["x"][file] = [x] * len(fields)
                    else:
                        inferred_properties["x"][file] = x
            # Otherwise use plot ID as file.
            else:
                inferred_properties["x"][self.plot_id] = x

        # Infer y.
        if y is None:
            inferred_properties["y"] = self._infer_y_from_data()
        # If y files not provided, use plot ID as file.
        elif not isinstance(y, dict):
            inferred_properties["y"] = {self.plot_id: y}

        return inferred_properties

    def _find_datapoints(self):
        result = {}
        for file, content in self.data.items():
            result[file] = get_datapoints(content)

        return result

    @staticmethod
    def infer_y_label(properties):
        y_label = properties.get("y_label", None)
        if y_label is not None:
            return y_label
        y = properties.get("y", None)
        if isinstance(y, str):
            return y
        if isinstance(y, list):
            return "y"
        if not isinstance(y, dict):
            return None

        fields = {field for _, field in _file_field(y)}
        if len(fields) == 1:
            return first(fields)
        return "y"

    @staticmethod
    def infer_x_label(properties):
        x_label = properties.get("x_label", None)
        if x_label is not None:
            return x_label

        x = properties.get("x", None)
        if not isinstance(x, dict):
            return INDEX

        fields = {field for _, field in _file_field(x)}
        if len(fields) == 1:
            return first(fields)
        return "x"

    def flat_datapoints(self, revision):  # noqa: C901, PLR0912
        file2datapoints, properties = self.convert()

        props_update: dict[str, Union[str, list[dict[str, str]]]] = {}

        xs = list(_get_xs(properties, file2datapoints))

        # assign "step" if no x provided
        if not xs:
            x_file, x_field = None, INDEX
        else:
            x_file, x_field = xs[0]

        num_xs = len(xs)
        multiple_x_fields = num_xs > 1 and len({x[1] for x in xs}) > 1
        props_update["x"] = "dvc_inferred_x_value" if multiple_x_fields else x_field

        ys = list(_get_ys(properties, file2datapoints))

        num_ys = len(ys)
        if num_xs > 1 and num_xs != num_ys:
            raise DvcException(
                "Cannot have different number of x and y data sources. Found "
                f"{num_xs} x and {num_ys} y data sources."
            )

        all_datapoints = []
        if ys:
            _all_y_files, _all_y_fields = list(zip(*ys))
            all_y_fields = set(_all_y_fields)
            all_y_files = set(_all_y_files)
        else:
            all_y_files = set()
            all_y_fields = set()

        # override to unified y field name if there are different y fields
        if len(all_y_fields) > 1:
            props_update["y"] = "dvc_inferred_y_value"
        else:
            props_update["y"] = first(all_y_fields)

        # get common prefix to drop from file names
        if len(all_y_files) > 1:
            common_prefix_len = len(os.path.commonpath(list(all_y_files)))
        else:
            common_prefix_len = 0

        props_update["anchors_y_definitions"] = [
            {FILENAME: _get_short_y_file(y_file, common_prefix_len), FIELD: y_field}
            for y_file, y_field in ys
        ]

        for i, (y_file, y_field) in enumerate(ys):
            if num_xs > 1:
                x_file, x_field = xs[i]
            datapoints = [{**d} for d in file2datapoints.get(y_file, [])]

            if props_update.get("y") == "dvc_inferred_y_value":
                _update_from_field(
                    datapoints,
                    field="dvc_inferred_y_value",
                    source_field=y_field,
                )

            if x_field == INDEX and x_file is None:
                _update_from_index(datapoints, INDEX)
            else:
                x_datapoints = file2datapoints.get(x_file, [])
                try:
                    _update_from_field(
                        datapoints,
                        field="dvc_inferred_x_value" if multiple_x_fields else x_field,
                        source_datapoints=x_datapoints,
                        source_field=x_field,
                    )
                except IndexError:
                    raise DvcException(  # noqa: B904
                        f"Cannot join '{x_field}' from '{x_file}' and "
                        f"'{y_field}' from '{y_file}'. "
                        "They have to have same length."
                    )

            _update_all(
                datapoints,
                update_dict={
                    REVISION: revision,
                    FILENAME: _get_short_y_file(y_file, common_prefix_len),
                    FIELD: y_field,
                },
            )

            all_datapoints.extend(datapoints)

        if not all_datapoints:
            return [], {}

        properties = properties | props_update

        return all_datapoints, properties

    def convert(self):
        """
        Convert the data. Fill necessary fields ('x', 'y') and return both
        generated datapoints and updated properties. `x`, `y` values and labels
        are inferred and always provided.
        """
        inferred_properties = self._infer_x_y()

        datapoints = self._find_datapoints()
        properties = self.properties | inferred_properties

        properties["y_label"] = self.infer_y_label(properties)
        properties["x_label"] = self.infer_x_label(properties)

        return datapoints, properties


def _get_short_y_file(y_file, common_prefix_len):
    return y_file[common_prefix_len:].strip("/\\")


def _update_from_field(
    target_datapoints: list[dict],
    field: str,
    source_datapoints: Optional[list[dict]] = None,
    source_field: Optional[str] = None,
):
    if source_datapoints is None:
        source_datapoints = target_datapoints
    if source_field is None:
        source_field = field

    if len(source_datapoints) != len(target_datapoints):
        raise IndexError("Source and target datapoints must have the same length")

    for index, datapoint in enumerate(target_datapoints):
        source_datapoint = source_datapoints[index]
        if source_field in source_datapoint:
            datapoint[field] = source_datapoint[source_field]


def _update_from_index(datapoints: list[dict], new_field: str):
    for index, datapoint in enumerate(datapoints):
        datapoint[new_field] = index


def _update_all(datapoints: list[dict], update_dict: dict):
    for datapoint in datapoints:
        datapoint.update(update_dict)
