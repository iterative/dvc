import sys

import pytest

from ..utils.test_hydra import hydra_setup


@pytest.mark.parametrize(
    "changes, expected",
    [
        [["foo=baz"], "foo: baz\ngoo:\n  bag: 3.0\nlorem: false"],
        [["params.yaml:foo=baz"], "foo: baz\ngoo:\n  bag: 3.0\nlorem: false"],
    ],
)
def test_modify_params(params_repo, dvc, changes, expected):
    dvc.experiments.run(params=changes)
    # pylint: disable=unspecified-encoding
    with open("params.yaml", mode="r") as fobj:
        assert fobj.read().strip() == expected


@pytest.mark.parametrize(
    "hydra_enabled",
    [
        pytest.param(
            True,
            marks=pytest.mark.skipif(
                sys.version_info >= (3, 11), reason="unsupported on 3.11"
            ),
        ),
        False,
    ],
)
@pytest.mark.parametrize(
    "config_dir,config_name",
    [
        (None, None),
        (None, "bar"),
        ("conf", "bar"),
    ],
)
def test_hydra_compose_and_dump(
    tmp_dir, params_repo, dvc, hydra_enabled, config_dir, config_name
):
    hydra_setup(
        tmp_dir,
        config_dir=config_dir or "conf",
        config_name=config_name or "config",
    )

    dvc.experiments.run()
    assert (tmp_dir / "params.yaml").parse() == {
        "foo": [{"bar": 1}, {"baz": 2}],
        "goo": {"bag": 3.0},
        "lorem": False,
    }

    with dvc.config.edit() as conf:
        if hydra_enabled:
            conf["hydra"]["enabled"] = True
        if config_dir is not None:
            conf["hydra"]["config_dir"] = config_dir
        if config_name is not None:
            conf["hydra"]["config_name"] = config_name

    dvc.experiments.run()

    if hydra_enabled:
        assert (tmp_dir / "params.yaml").parse() == {
            "db": {"driver": "mysql", "user": "omry", "pass": "secret"},
        }

        dvc.experiments.run(params=["db=postgresql"])
        assert (tmp_dir / "params.yaml").parse() == {
            "db": {
                "driver": "postgresql",
                "user": "foo",
                "pass": "bar",
                "timeout": 10,
            }
        }
    else:
        assert (tmp_dir / "params.yaml").parse() == {
            "foo": [{"bar": 1}, {"baz": 2}],
            "goo": {"bag": 3.0},
            "lorem": False,
        }
