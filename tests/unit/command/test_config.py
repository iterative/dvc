import pytest

from dvc.cli import DvcParserError, parse_args


@pytest.mark.parametrize(
    "name", ["way.too.long", "no_option", "remote.way.too.long"],
)
def test_config_bad_name(name):
    with pytest.raises(DvcParserError):
        parse_args(["config", name])
