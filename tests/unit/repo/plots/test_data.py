from collections import OrderedDict
from typing import Dict, List

import pytest

from dvc.repo.plots.data import _lists, to_datapoints

# TODO do we need those tests?


@pytest.mark.parametrize(
    "dictionary, expected_result",
    [
        ({}, []),
        ({"x": ["a", "b", "c"]}, [["a", "b", "c"]]),
        (
            OrderedDict([("x", {"y": ["a", "b"]}), ("z", {"w": ["c", "d"]})]),
            [["a", "b"], ["c", "d"]],
        ),
    ],
)
def test_finding_lists(dictionary, expected_result):
    result = _lists(dictionary)

    assert list(result) == expected_result


def test_find_data_in_dict(tmp_dir):
    m1 = [{"accuracy": 1, "loss": 2}, {"accuracy": 3, "loss": 4}]
    m2 = [{"x": 1}, {"x": 2}]
    dmetric = OrderedDict([("t1", m1), ("t2", m2)])

    def points_with(datapoints: List, additional_info: Dict):
        for datapoint in datapoints:
            datapoint.update(additional_info)

        return datapoints

    assert list(
        map(dict, to_datapoints(dmetric, "revision", "file"))
    ) == points_with(m1, {"rev": "revision"})
    assert list(
        map(dict, to_datapoints(dmetric, "revision", "file", fields={"x"}))
    ) == points_with(m2, {"rev": "revision"})
