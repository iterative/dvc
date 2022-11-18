from copy import deepcopy
from typing import Any, Dict, Iterable, List, Tuple, Union

from funcy import first, last

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


def _verify_field(file2datapoints: Dict[str, List], filename: str, field: str):
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


def _is_datapoints(lst: List[Dict]):
    """
    check if dict keys match, datapoints with different keys mgiht lead
    to unexpected behavior
    """

    return all(isinstance(item, dict) for item in lst) and set(
        first(lst).keys()
    ) == {key for keys in lst for key in keys}


def get_datapoints(file_content: Dict):
    result: List[Dict[str, Any]] = []
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
        self, plot_id: str, data: Dict = None, properties: Dict = None
    ):
        super().__init__(plot_id, data, properties)
        self.plot_id = plot_id
        self.inferred_properties: Dict = {}

        # TODO we should be handling that in `convert`,
        #      to avoid stateful `self.inferred_properties`
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
            else:
                x_label = INDEX_FIELD
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

        all_datapoints = []
        all_y_fields = {y_field for _, y_field in ys}

        # override to unified y field name if there are different y fields
        if len(all_y_fields) > 1:
            props_update["y"] = "dvc_inferred_y_value"
        else:
            props_update["y"] = first(all_y_fields)

        for y_file, y_field in ys:
            datapoints = deepcopy(file2datapoints.get(y_file, []))

            if props_update.get("y", None) == "dvc_inferred_y_value":
                _update_from_field(
                    datapoints,
                    field="dvc_inferred_y_value",
                    source_field=y_field,
                )

            if x_field == INDEX_FIELD and x_file is None:
                _update_from_index(datapoints, INDEX_FIELD)
            else:
                x_datapoints = file2datapoints.get(x_file, [])
                try:
                    _update_from_field(
                        datapoints,
                        field=x_field,
                        source_datapoints=x_datapoints,
                    )
                except IndexError:
                    raise DvcException(
                        f"Cannot join '{x_field}' from '{x_file}' and "
                        "'{y_field}' from '{y_file}'. "
                        "They have to have same length."
                    )

            _update_all(
                datapoints,
                update_dict={
                    VERSION_FIELD: {
                        "revision": revision,
                        FILENAME_FIELD: y_file,
                        "field": y_field,
                    }
                },
            )

            all_datapoints.extend(datapoints)

        if not all_datapoints:
            return [], {}

        properties = {**properties, **props_update}

        return all_datapoints, properties

    def convert(
        self,
    ):
        """
        Convert the data. Fill necessary fields ('x', 'y') and return both
        generated datapoints and updated properties. If `x` is not provided,
        leave it as None, fronteds should handle it.

        NOTE: Studio uses this method.
              The only thing studio FE handles is filling `x` and `y`.
              `x/y_label` should be filled here.

              Datapoints are not stripped according to config, because users
              might be utilizing other fields in their custom plots.
        """
        datapoints = self._find_datapoints()
        properties = {**self.properties, **self.inferred_properties}

        properties["y_label"] = self.infer_y_label(properties)
        properties["x_label"] = self.infer_x_label(properties)

        return datapoints, properties


def _update_from_field(
    target_datapoints: List[Dict],
    field: str,
    source_datapoints: List[Dict] = None,
    source_field: str = None,
):
    if source_datapoints is None:
        source_datapoints = target_datapoints
    if source_field is None:
        source_field = field

    if len(source_datapoints) != len(target_datapoints):
        raise IndexError(
            "Source and target datapoints must have the same length"
        )

    for index, datapoint in enumerate(target_datapoints):
        source_datapoint = source_datapoints[index]
        if source_field in source_datapoint:
            datapoint[field] = source_datapoint[source_field]


def _update_from_index(datapoints: List[Dict], new_field: str):
    for index, datapoint in enumerate(datapoints):
        datapoint[new_field] = index


def _update_all(datapoints: List[Dict], update_dict: Dict):

    for datapoint in datapoints:
        datapoint.update(update_dict)
