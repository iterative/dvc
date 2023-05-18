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
