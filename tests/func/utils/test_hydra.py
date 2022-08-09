import pytest

from dvc.exceptions import InvalidArgumentError
from dvc.utils.hydra import apply_overrides


@pytest.mark.parametrize("suffix", ["yaml", "toml", "json"])
@pytest.mark.parametrize(
    "overrides, expected",
    [
        # Overriding
        (["foo=baz"], {"foo": "baz", "goo": {"bag": 3.0}, "lorem": False}),
        (["foo=baz", "goo=bar"], {"foo": "baz", "goo": "bar", "lorem": False}),
        (
            ["foo.0=bar"],
            {"foo": ["bar", {"baz": 2}], "goo": {"bag": 3.0}, "lorem": False},
        ),
        (
            ["foo.1.baz=3"],
            {
                "foo": [{"bar": 1}, {"baz": 3}],
                "goo": {"bag": 3.0},
                "lorem": False,
            },
        ),
        (
            ["goo.bag=4.0"],
            {
                "foo": [{"bar": 1}, {"baz": 2}],
                "goo": {"bag": 4.0},
                "lorem": False,
            },
        ),
        (
            ["++goo={bag: 1, b: 2}"],
            {
                "foo": [{"bar": 1}, {"baz": 2}],
                "goo": {"bag": 1, "b": 2},
                "lorem": False,
            },
        ),
        # 6129
        (
            ["lorem="],
            {
                "foo": [{"bar": 1}, {"baz": 2}],
                "goo": {"bag": 3.0},
                "lorem": "",
            },
        ),
        # 6129
        (
            ["lorem=null"],
            {
                "foo": [{"bar": 1}, {"baz": 2}],
                "goo": {"bag": 3.0},
                "lorem": None,
            },
        ),
        # 5868
        (
            ["lorem=1992-11-20"],
            {
                "foo": [{"bar": 1}, {"baz": 2}],
                "goo": {"bag": 3.0},
                "lorem": "1992-11-20",
            },
        ),
        # 5868
        (
            ["lorem='1992-11-20'"],
            {
                "foo": [{"bar": 1}, {"baz": 2}],
                "goo": {"bag": 3.0},
                "lorem": "1992-11-20",
            },
        ),
        # Appending
        (
            ["+a=1"],
            {
                "foo": [{"bar": 1}, {"baz": 2}],
                "goo": {"bag": 3.0},
                "lorem": False,
                "a": 1,
            },
        ),
        # Removing
        (["~foo"], {"goo": {"bag": 3.0}, "lorem": False}),
    ],
)
def test_apply_overrides(tmp_dir, suffix, overrides, expected):
    if suffix == "toml":
        if overrides in [
            ["foo=baz"],
            ["foo.0=bar"],
            ["foo=baz", "goo=bar"],
            ["lorem=null"],
        ]:
            # TOML dumper breaks when overriding a list/dict with other type
            # or when handling `null` values.
            pytest.xfail()

    params_file = tmp_dir / f"params.{suffix}"
    params_file.dump(
        {"foo": [{"bar": 1}, {"baz": 2}], "goo": {"bag": 3.0}, "lorem": False}
    )
    apply_overrides(path=params_file.name, overrides=overrides)
    assert params_file.parse() == expected


@pytest.mark.parametrize(
    "overrides",
    [["foobar=2"], ["lorem=3,2"], ["+lorem=3"], ["foo[0]=bar"]],
)
def test_invalid_overrides(tmp_dir, overrides):
    params_file = tmp_dir / "params.yaml"
    params_file.dump(
        {"foo": [{"bar": 1}, {"baz": 2}], "goo": {"bag": 3.0}, "lorem": False}
    )
    with pytest.raises(InvalidArgumentError):
        apply_overrides(path=params_file.name, overrides=overrides)
