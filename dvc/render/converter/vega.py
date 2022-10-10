from typing import Any, Dict, Iterable, List, Tuple, Union

from funcy import first, group_values, last

from dvc.exceptions import DvcException
from dvc.render import FILENAME_FIELD, INDEX_FIELD, VERSION_FIELD

from . import Converter


def _lists(blob: Union[Dict, List]) -> Iterable[List]:
    if isinstance(blob, list):
        yield blob
    else:
        for _, value in blob.items():
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


def _find(
    filename: str,
    field: str,
    data_series: List[Tuple[str, str, Any]],
):
    for data_file, data_field, data in data_series:
        if data_file == filename and data_field == field:
            return data_file, data_field, data
    return None


def _get_x(properties: Dict, data_series: List[Tuple[str, str, Any]]):
    x = properties.get("x", None)
    if x is not None and isinstance(x, dict):
        filename, field = first(_file_field(x))
        return _find(filename, field, data_series)
    return None


def _get_ys(properties, data_series: List[Tuple[str, str, Any]]):
    y = properties.get("y", None)
    if y is not None:
        for filename, field in _file_field(y):
            result = _find(filename, field, data_series)
            if result is not None:
                yield result


def _is_datapoints(lst: List):
    # check if dict keys match, datapoints with different keys mgiht lead
    # to unexpected behavior
    return all(isinstance(item, dict) for item in lst) and set(
        first(lst).keys()
    ) == {key for keys in lst for key in keys}


def get_data_series(file_content: Dict):
    data_series = {}
    for lst in _lists(file_content):
        if _is_datapoints(lst):
            data_series.update(
                group_values(
                    (key, value)
                    for datapoint in lst
                    for key, value in datapoint.items()
                )
            )
    return dict(data_series)


class VegaConverter(Converter):
    """
    Class that takes care of converting unspecified data blob
    (Dict or List[Dict]) into datapoints (List[Dict]).
    If some properties that are required by Template class are missing
    ('x', 'y') it will attempt to fill in the blanks.
    """

    def __init__(
        self, plot_id: str, data: Dict = None, properties: Dict = None
    ):
        super().__init__(plot_id, data, properties)
        self.plot_id = plot_id
        self.inferred_properties: Dict = {}

        self._infer_x_y()

    def _infer_y_from_data(self):
        if self.plot_id in self.data:
            for lst in _lists(self.data[self.plot_id]):
                if all(isinstance(item, dict) for item in lst):
                    datapoint = first(lst)
                    field = last(datapoint.keys())
                    self.inferred_properties["y"] = {self.plot_id: field}
                    break

    def _infer_x_y(self):
        def _infer_files(from_name, to_name):
            from_value = self.properties.get(from_name, None)
            to_value = self.properties.get(to_name, None)

            if isinstance(to_value, str):
                self.inferred_properties[to_name] = {}
                if isinstance(from_value, dict):
                    for file in from_value.keys():
                        self.inferred_properties[to_name][file] = to_value
                else:
                    self.inferred_properties[to_name][self.plot_id] = to_value
            else:
                self.inferred_properties[to_name] = to_value

        _infer_files("y", "x")
        _infer_files("x", "y")

        if self.inferred_properties.get("y", None) is None:
            self._infer_y_from_data()

    def _find_series(self) -> List[Tuple[str, str, List]]:
        x = self.inferred_properties.get("x", None)
        y = self.inferred_properties.get("y", None)
        file_fields = list(_file_field(x, y))

        result = []
        for file, content in self.data.items():
            for field, data in get_data_series(content).items():
                if ((file, field) in file_fields) and len(data) > 0:
                    result.append((file, field, data))
        return result

    def flat_datapoints(self, revision):
        def _datapoint(d: Dict, revision, filename, field):
            d.update(
                {
                    VERSION_FIELD: {
                        "revision": revision,
                        FILENAME_FIELD: filename,
                        "field": field,
                    }
                }
            )
            return d

        datas, properties = self.convert()

        x = _get_x(properties, datas)
        ys = list(_get_ys(properties, datas))
        if x and not ys:
            file, field, data = x
            return [
                _datapoint({field: val}, revision, file, field) for val in data
            ], {**properties, **{"x": field}}

        dps = []
        props_update = {}
        all_y_fields = {y_field for _, y_field, _ in ys}
        for y_file, y_field, y_data in ys:
            y_value_name = y_field

            # assign "step" if no x provided
            if not x:
                x_file, x_field, x_data = (
                    None,
                    INDEX_FIELD,
                    list(range(len(y_data))),
                )
            else:
                x_file, x_field, x_data = x
            props_update["x"] = x_field

            # override to unified y field name if there are multiple y fields
            if len(all_y_fields) > 1:
                y_value_name = "dvc_inferred_y_value"
                props_update["y"] = "dvc_inferred_y_value"
                if "y_label" not in properties:
                    props_update["y_label"] = "y"
            else:
                props_update["y"] = y_field

            try:
                dps.extend(
                    [
                        _datapoint(
                            {y_value_name: y_val, x_field: x_data[index]},
                            revision,
                            y_file,
                            y_field,
                        )
                        for index, y_val in enumerate(y_data)
                    ]
                )
            except IndexError:
                raise DvcException(
                    f"Number of values in '{x_field}' field from '{x_file}' "
                    f"and '{y_field}' from '{y_file}' columns do not match."
                )

        if not dps:
            return [], {}

        return dps, {**properties, **props_update}

    def convert(
        self,
    ):
        """
        Convert the data. Fill necessary fields ('x', 'y') and return both
        generated datapoints and updated properties.
        """
        data_series = self._find_series()

        return data_series, {
            **self.properties,
            **self.inferred_properties,
        }
