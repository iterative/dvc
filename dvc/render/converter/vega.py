from typing import Any, Dict, Iterable, List, Tuple, Union

from funcy import first, group_values, last

from dvc.exceptions import DvcException
from dvc.render import FILENAME_FIELD, INDEX_FIELD, VERSION_FIELD

from . import Converter


class FieldNotFoundError(DvcException):
    def __init__(self, expected_field, found_fields):
        found_str = ", ".join(found_fields)
        super().__init__(
            f"Could not find provided field ('{expected_field}') "
            f"in data fields ('{found_str}')."
        )


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


def _verify_field(file2datapoints: List, filename: str, field: str):
    if filename in file2datapoints:
        datapoint = first(file2datapoints[filename])
        if field not in datapoint:
            raise FieldNotFoundError(field, datapoint.keys())
    return


def _get_x(properties: Dict, file2datapoints: Dict[str, List[Dict]]):
    x = properties.get("x", None)
    if x is not None and isinstance(x, dict):
        filename, field = first(_file_field(x))
        _verify_field(file2datapoints, filename, field)
        return filename, field
    return None


def _get_ys(properties, file2datapoints: Dict[str, List[Dict]]):
    y = properties.get("y", None)
    if y is not None:
        for filename, field in _file_field(y):
            _verify_field(file2datapoints, filename, field)
            yield filename, field


def _is_datapoints(lst: List):
    # check if dict keys match, datapoints with different keys mgiht lead
    # to unexpected behavior
    return all(isinstance(item, dict) for item in lst) and set(
        first(lst).keys()
    ) == {key for keys in lst for key in keys}


def get_datapoints(file_content: Dict):
    # TODO optimize
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
    result = []
    for field, data in data_series.items():
        for index, value in enumerate(data):
            if len(result) <= index:
                result.append({})
            result[index][field] = value
    return result


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

    def _find_datapoints(self):
        result = {}
        for file, content in self.data.items():
            result[file] = get_datapoints(content)

        return result

    @staticmethod
    def infer_y_label(properties):
        y_label = properties.get("y_label", None)
        if y_label is None:
            y = properties.get("y", None)
            if isinstance(y, dict):
                fields = {field for _, field in _file_field(y)}
                if len(fields) == 1:
                    y_label = first(fields)
                else:
                    y_label = "y"
            elif isinstance(y, list):
                y_label = "y"
            elif isinstance(y, str):
                y_label = y

        return y_label

    @staticmethod
    def infer_x_label(properties):
        x_label = properties.get("x_label", None)
        if x_label is None:
            x = properties.get("x", None)
            if isinstance(x, dict):
                fields = {field for _, field in _file_field(x)}
                if len(fields) == 1:
                    x_label = first(fields)
                else:
                    x_label = "x"
            elif isinstance(x, list):
                x_label = "x"
            elif isinstance(x, str):
                x_label = x
        return x_label

    def flat_datapoints(self, revision):

        file2datapoints, properties = self.convert()

        props_update = {}

        x = _get_x(properties, file2datapoints)

        # assign "step" if no x provided
        if not x:
            x_file, x_field = (
                None,
                INDEX_FIELD,
            )
        else:
            x_file, x_field = x
        props_update["x"] = x_field

        ys = list(_get_ys(properties, file2datapoints))

        dps = []
        all_y_fields = {y_field for _, y_field in ys}
        for y_file, y_field in ys:
            y_value_name = y_field

            # override to unified y field name if there are multiple y fields
            y_value_name = None
            if len(all_y_fields) > 1:
                y_value_name = "dvc_inferred_y_value"
                props_update["y"] = "dvc_inferred_y_value"
                if "y_label" not in properties:
                    props_update["y_label"] = "y"
            else:
                props_update["y"] = y_field

            try:
                for index, datapoint in enumerate(
                    file2datapoints.get(y_file, [])
                ):
                    tmp = {**datapoint}
                    if y_value_name:
                        tmp[y_value_name] = datapoint[y_field]
                        del tmp[y_field]
                    # TODO
                    if x_field == INDEX_FIELD and x_file is None:
                        tmp[x_field] = index
                    else:
                        # TODO what if there is no x data?
                        tmp[x_field] = file2datapoints.get(x_file, [])[index][
                            x_field
                        ]
                    tmp.update(
                        {
                            VERSION_FIELD: {
                                "revision": revision,
                                FILENAME_FIELD: y_file,
                                "field": y_field,
                            }
                        }
                    )
                    dps.append(tmp)
            # TODO better handling
            except IndexError:
                raise DvcException(
                    f"Number of values in '{x_field}' field from '{x_file}' "
                    f"and '{y_field}' from '{y_file}' columns do not match."
                )

        if not dps:
            return [], {}

        properties = {**properties, **props_update}
        properties["y_label"] = self.infer_y_label(properties)
        properties["x_label"] = self.infer_x_label(properties)

        return dps, properties

    def convert(
        self,
    ):
        """
        Convert the data. Fill necessary fields ('x', 'y') and return both
        generated datapoints and updated properties.
        """
        datapoints = self._find_datapoints()

        return datapoints, {
            **self.properties,
            **self.inferred_properties,
        }
