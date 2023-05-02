import pytest

from dvc.exceptions import InvalidArgumentError


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
    from dvc.utils.hydra import apply_overrides

    if suffix == "toml" and overrides in [
        ["foo=baz"],
        ["foo.0=bar"],
        ["foo=baz", "goo=bar"],
        ["lorem=null"],
    ]:
        pytest.skip(
            "TOML dumper breaks when overriding a list/dict with other type or"
            " when handling `null` values."
        )

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
    from dvc.utils.hydra import apply_overrides

    params_file = tmp_dir / "params.yaml"
    params_file.dump(
        {"foo": [{"bar": 1}, {"baz": 2}], "goo": {"bag": 3.0}, "lorem": False}
    )
    with pytest.raises(InvalidArgumentError):
        apply_overrides(path=params_file.name, overrides=overrides)


def hydra_setup(tmp_dir, config_dir, config_name):
    config_dir = tmp_dir / config_dir
    (config_dir / "db").mkdir(parents=True)
    (config_dir / f"{config_name}.yaml").dump({"defaults": [{"db": "mysql"}]})
    (config_dir / "db" / "mysql.yaml").dump(
        {"driver": "mysql", "user": "omry", "pass": "secret"}
    )
    (config_dir / "db" / "postgresql.yaml").dump(
        {"driver": "postgresql", "user": "foo", "pass": "bar", "timeout": 10}
    )
    return str(config_dir)


@pytest.mark.parametrize("suffix", ["yaml", "toml", "json"])
@pytest.mark.parametrize(
    "overrides,expected",
    [
        ([], {"db": {"driver": "mysql", "user": "omry", "pass": "secret"}}),
        (
            ["db=postgresql"],
            {
                "db": {
                    "driver": "postgresql",
                    "user": "foo",
                    "pass": "bar",
                    "timeout": 10,
                }
            },
        ),
        (
            ["db=postgresql", "db.timeout=20"],
            {
                "db": {
                    "driver": "postgresql",
                    "user": "foo",
                    "pass": "bar",
                    "timeout": 20,
                }
            },
        ),
    ],
)
def test_compose_and_dump(tmp_dir, suffix, overrides, expected):
    from dvc.utils.hydra import compose_and_dump

    config_name = "config"
    output_file = tmp_dir / f"params.{suffix}"
    config_dir = hydra_setup(tmp_dir, "conf", "config")
    compose_and_dump(output_file, config_dir, config_name, overrides)
    assert output_file.parse() == expected


def test_compose_and_dump_yaml_handles_string(tmp_dir):
    """Regression test for https://github.com/iterative/dvc/issues/8583"""
    from dvc.utils.hydra import compose_and_dump

    config = tmp_dir / "conf" / "config.yaml"
    config.parent.mkdir()
    config.write_text("foo: 'no'\n")
    output_file = tmp_dir / "params.yaml"
    compose_and_dump(output_file, str(config.parent), "config", [])
    assert output_file.read_text() == "foo: 'no'\n"


def test_compose_and_dump_resolves_interpolation(tmp_dir):
    """Regression test for https://github.com/iterative/dvc/issues/9196"""
    from dvc.utils.hydra import compose_and_dump

    config = tmp_dir / "conf" / "config.yaml"
    config.parent.mkdir()
    config.dump({"data": {"root": "path/to/root", "raw": "${.root}/raw"}})
    output_file = tmp_dir / "params.yaml"
    compose_and_dump(output_file, str(config.parent), "config", [])
    assert output_file.parse() == {
        "data": {"root": "path/to/root", "raw": "path/to/root/raw"}
    }


@pytest.mark.parametrize(
    "overrides, expected",
    [
        (
            {"params.yaml": ["defaults/foo=1,2"]},
            [
                {"params.yaml": ["defaults/foo=1"]},
                {"params.yaml": ["defaults/foo=2"]},
            ],
        ),
        (
            {"params.yaml": ["+foo=1,2", "~bar", "++foobar=5,6"]},
            [
                {"params.yaml": ["+foo=1", "~bar=null", "++foobar=5"]},
                {"params.yaml": ["+foo=1", "~bar=null", "++foobar=6"]},
                {"params.yaml": ["+foo=2", "~bar=null", "++foobar=5"]},
                {"params.yaml": ["+foo=2", "~bar=null", "++foobar=6"]},
            ],
        ),
        (
            {"params.yaml": ["foo=1,2", "bar=3,4"]},
            [
                {"params.yaml": ["foo=1", "bar=3"]},
                {"params.yaml": ["foo=1", "bar=4"]},
                {"params.yaml": ["foo=2", "bar=3"]},
                {"params.yaml": ["foo=2", "bar=4"]},
            ],
        ),
        (
            {"params.yaml": ["foo=choice(1,2)"]},
            [{"params.yaml": ["foo=1"]}, {"params.yaml": ["foo=2"]}],
        ),
        (
            {"params.yaml": ["foo=range(1, 3)"]},
            [{"params.yaml": ["foo=1"]}, {"params.yaml": ["foo=2"]}],
        ),
        (
            {"params.yaml": ["foo=1,2"], "others.yaml": ["bar=3"]},
            [
                {"params.yaml": ["foo=1"], "others.yaml": ["bar=3"]},
                {"params.yaml": ["foo=2"], "others.yaml": ["bar=3"]},
            ],
        ),
        (
            {"params.yaml": ["foo=1,2"], "others.yaml": ["bar=3,4"]},
            [
                {"params.yaml": ["foo=1"], "others.yaml": ["bar=3"]},
                {"params.yaml": ["foo=1"], "others.yaml": ["bar=4"]},
                {"params.yaml": ["foo=2"], "others.yaml": ["bar=3"]},
                {"params.yaml": ["foo=2"], "others.yaml": ["bar=4"]},
            ],
        ),
    ],
)
def test_hydra_sweeps(overrides, expected):
    from dvc.utils.hydra import get_hydra_sweeps

    assert get_hydra_sweeps(overrides) == expected


def test_invalid_sweep():
    from dvc.utils.hydra import get_hydra_sweeps

    with pytest.raises(InvalidArgumentError):
        get_hydra_sweeps({"params.yaml": ["foo=glob(*)"]})
