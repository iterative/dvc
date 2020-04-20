import json

import pytest

from dvc.repo.plot import _parse_json


@pytest.mark.parametrize(
    "path,expected_result",
    [
        ("$.some.path[*].a", [{"a": 1}, {"a": 4}]),
        ("$.some.path", [{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}]),
    ],
)
def test_parse_json(path, expected_result):
    value = {
        "some": {"path": [{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}]}
    }

    result = _parse_json(json.dumps(value), path)

    assert result == expected_result
