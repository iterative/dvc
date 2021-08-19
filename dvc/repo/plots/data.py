from copy import copy, deepcopy

from funcy import first

from dvc.exceptions import DvcException

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
            "https://man.dvc.org/plot for supported data formats."
        )


def _filter_fields(data_points, filename, revision, fields=None, **kwargs):
    if not fields:
        return data_points
    assert isinstance(fields, set)

    new_data = []
    for data_point in data_points:
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


def _find_data(data, fields=None, **kwargs):
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


def _append_index(data_points, append_index=False, **kwargs):
    if not append_index or INDEX_FIELD in first(data_points).keys():
        return data_points

    for index, data_point in enumerate(data_points):
        data_point[INDEX_FIELD] = index
    return data_points


def _append_revision(data_points, revision, **kwargs):
    for data_point in data_points:
        data_point[REVISION_FIELD] = revision
    return data_points


def to_datapoints(data, revision, filename, **kwargs):
    # TODO data assumes single file, but it does not have to be, assert?
    result = deepcopy(data)
    for processor in [
        _find_data,
        _filter_fields,
        _append_index,
        _append_revision,
    ]:
        result = processor(
            result, revision=revision, filename=filename, **kwargs
        )

    return result
