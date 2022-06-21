from itertools import takewhile

import pytest

from dvc.cli import parse_args


def _id_gen(val) -> str:
    if isinstance(val, list):
        return "-".join(takewhile(lambda v: not v.startswith("-"), val))
    return str(val)


@pytest.mark.parametrize(
    "args, key",
    [
        (["dag", "--show-md"], "markdown"),
        (["diff", "--show-json"], "json"),
        (["diff", "--show-md"], "markdown"),
        (["experiments", "diff", "--show-json"], "json"),
        (["experiments", "diff", "--show-md"], "markdown"),
        (["experiments", "show", "--show-json"], "json"),
        (["experiments", "show", "--show-csv"], "csv"),
        (["experiments", "show", "--show-md"], "markdown"),
        (["ls", "--show-json", "."], "json"),
        (["metrics", "diff", "--show-json"], "json"),
        (["metrics", "diff", "--show-md"], "markdown"),
        (["metrics", "show", "--show-json"], "json"),
        (["metrics", "show", "--show-md"], "markdown"),
        (["params", "diff", "--show-json"], "json"),
        (["params", "diff", "--show-md"], "markdown"),
        (["status", "--show-json"], "json"),
        (["plots", "show", "--show-json"], "json"),
        (["plots", "diff", "--show-json"], "json"),
        (["exp", "list", "--names-only"], "name_only"),
        (["stage", "list", "--names-only"], "name_only"),
    ],
    ids=_id_gen,
)
def test_backward_compat_flags(args, key):
    """Test support for flags kept for backward compatibility."""
    cli_args = parse_args(args)
    d = vars(cli_args)
    assert d[key] is True
