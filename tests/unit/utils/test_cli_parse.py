import pytest

from dvc.utils.cli_parse import parse_params, to_path_overrides


def test_parse_params():
    assert parse_params(
        [
            "param1",
            "file1:param1,param2",
            "file2:param2",
            "file1:param3,param4,",
            "param2,param10",
            "param3,",
            "file3:",
        ]
    ) == [
        {"params.yaml": ["param1", "param2", "param10", "param3"]},
        {"file1": ["param1", "param2", "param3", "param4"]},
        {"file2": ["param2"]},
        {"file3": []},
    ]


@pytest.mark.parametrize(
    "params,expected",
    [
        (["foo=1"], {"params.yaml": ["foo=1"]}),
        (["foo={bar: 1}"], {"params.yaml": ["foo={bar: 1}"]}),
        (["foo.0=bar"], {"params.yaml": ["foo.0=bar"]}),
        (["params.json:foo={bar: 1}"], {"params.json": ["foo={bar: 1}"]}),
        (
            ["params.json:foo={bar: 1}", "baz=2", "goo=3"],
            {
                "params.json": ["foo={bar: 1}"],
                "params.yaml": ["baz=2", "goo=3"],
            },
        ),
    ],
)
def test_to_path_overrides(params, expected):
    assert to_path_overrides(params) == expected
