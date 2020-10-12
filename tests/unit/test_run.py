import pytest

from dvc.repo.run import is_valid_name, parse_params


def test_parse_params():
    assert parse_params(
        [
            "param1",
            "file1:param1,param2",
            "file2:param2",
            "file1:param2,param3,",
            "param1,param2",
            "param3,",
            "file3:",
        ]
    ) == [
        "param1",
        {"file1": ["param1", "param2"]},
        {"file2": ["param2"]},
        {"file1": ["param2", "param3"]},
        "param1",
        "param2",
        "param3",
        {"file3": []},
    ]


@pytest.mark.parametrize("name", ["copy_name", "copy-name", "copyName", "12"])
def test_valid_stage_names(name):
    assert is_valid_name(name)


@pytest.mark.parametrize("name", ["copy$name", "copy-name?", "copy-name@v1"])
def test_invalid_stage_names(name):
    assert not is_valid_name(name)
