from collections import OrderedDict

import pytest

from dvc.repo.plots.data import _find_data, _lists


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


@pytest.mark.parametrize("fields", [{"x"}, set()])
def test_finding_data(fields):
    data = {"a": {"b": [{"x": 2, "y": 3}, {"x": 1, "y": 5}]}}

    result = _find_data(data, fields=fields)

    assert result == [{"x": 2, "y": 3}, {"x": 1, "y": 5}]
