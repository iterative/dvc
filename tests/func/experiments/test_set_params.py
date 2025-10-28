import pytest

from dvc.exceptions import InvalidArgumentError
from tests.func.utils.test_hydra import hydra_setup


@pytest.mark.parametrize(
    "changes, expected",
    [
        (["foo=baz"], "foo: baz\ngoo:\n  bag: 3.0\nlorem: false"),
        (["params.yaml:foo=baz"], "foo: baz\ngoo:\n  bag: 3.0\nlorem: false"),
    ],
)
def test_modify_params(params_repo, dvc, changes, expected):
    dvc.experiments.run(params=changes)
    with open("params.yaml") as fobj:
        assert fobj.read().strip() == expected


@pytest.mark.parametrize("hydra_enabled", [True, False])
@pytest.mark.parametrize(
    "config_dir,config_name",
    [
        (None, None),
        (None, "bar"),
        ("conf", "bar"),
    ],
)
@pytest.mark.parametrize("no_hydra", [True, False])
def test_hydra_compose_and_dump(
    tmp_dir, params_repo, dvc, hydra_enabled, config_dir, config_name, no_hydra
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

    dvc.experiments.run(no_hydra=no_hydra)

    if hydra_enabled and not no_hydra:
        assert (tmp_dir / "params.yaml").parse() == {
            "db": {"driver": "mysql", "user": "omry", "pass": "secret"},
        }

        dvc.experiments.run(params=["db=postgresql"], no_hydra=no_hydra)
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


@pytest.mark.parametrize(
    "hydra_enabled,overrides,expected",
    [
        (
            True,
            ["db=mysql,postgresql"],
            [
                {"params.yaml": ["db=mysql"]},
                {"params.yaml": ["db=postgresql"]},
            ],
        ),
        (
            False,
            ["foo=bar,baz"],
            [{"params.yaml": ["foo=bar"]}, {"params.yaml": ["foo=baz"]}],
        ),
        (
            False,
            [],
            [{}],
        ),
    ],
)
def test_hydra_sweep(
    tmp_dir, params_repo, dvc, mocker, hydra_enabled, overrides, expected
):
    patched = mocker.patch.object(dvc.experiments, "queue_one")

    if hydra_enabled:
        hydra_setup(tmp_dir, config_dir="conf", config_name="config")
        with dvc.config.edit() as conf:
            conf["hydra"]["enabled"] = True

    dvc.experiments.run(params=overrides, queue=True)

    assert patched.call_count == len(expected)
    for e in expected:
        patched.assert_any_call(
            mocker.ANY,
            params=e,
            targets=None,
            copy_paths=None,
            message=None,
            no_hydra=False,
        )


def test_hydra_sweep_requires_queue(params_repo, dvc):
    with pytest.raises(
        InvalidArgumentError,
        match="Sweep overrides can't be used without `--queue`",
    ):
        dvc.experiments.run(params=["db=mysql,postgresql"])


def test_hydra_sweep_prefix_name(tmp_dir, params_repo, dvc):
    prefix = "foo"
    db_values = ["mysql", "postgresql"]
    param = "+db=" + ",".join(db_values)
    dvc.experiments.run(params=[param], queue=True, name=prefix)
    expected_names = [f"{prefix}-{i + 1}" for i, _ in enumerate(db_values)]
    exp_names = [entry.name for entry in dvc.experiments.celery_queue.iter_queued()]
    for name, expected in zip(exp_names, expected_names):
        assert name == expected


def test_mixing_no_hydra_and_params_flags(tmp_dir, params_repo, dvc):
    # Passing no_hydra should not prevent user from
    # using --set-param on unmodified params.yaml
    hydra_setup(
        tmp_dir,
        config_dir="conf",
        config_name="config",
    )

    with dvc.config.edit() as conf:
        conf["hydra"]["enabled"] = True
        conf["hydra"]["config_dir"] = "conf"
        conf["hydra"]["config_name"] = "config"

    dvc.experiments.run(no_hydra=True, params=["goo.bag=10.0"])

    assert (tmp_dir / "params.yaml").parse() == {
        "foo": [{"bar": 1}, {"baz": 2}],
        "goo": {"bag": 10.0},
        "lorem": False,
    }


@pytest.mark.parametrize(
    "hydra_enabled,overrides,expected",
    [
        (
            True,
            ["db=mysql,postgresql"],
            [
                {"params.yaml": ["db=mysql"]},
                {"params.yaml": ["db=postgresql"]},
            ],
        ),
        (
            False,
            ["foo=bar,baz"],
            [{"params.yaml": ["foo=bar"]}, {"params.yaml": ["foo=baz"]}],
        ),
        (
            False,
            [],
            [{}],
        ),
    ],
)
@pytest.mark.parametrize("no_hydra", [True, False])
def test_mixing_no_hydra_and_sweeps(
    tmp_dir, params_repo, dvc, mocker, hydra_enabled, overrides, expected, no_hydra
):
    # Passing no_hydra should not prevent user from
    # queuing sweeps with --set-param and --queue
    patched = mocker.patch.object(dvc.experiments, "queue_one")

    if hydra_enabled:
        hydra_setup(tmp_dir, config_dir="conf", config_name="config")
        with dvc.config.edit() as conf:
            conf["hydra"]["enabled"] = True

    dvc.experiments.run(params=overrides, queue=True, no_hydra=no_hydra)

    assert patched.call_count == len(expected)
    for e in expected:
        patched.assert_any_call(
            mocker.ANY,
            params=e,
            targets=None,
            copy_paths=None,
            message=None,
            no_hydra=no_hydra,
        )
