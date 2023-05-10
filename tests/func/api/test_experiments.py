import pytest

from dvc import api
from dvc.repo.experiments.exceptions import ExperimentExistsError
from tests.unit.repo.experiments.conftest import exp_stage  # noqa: F401


def test_exp_save(tmp_dir, dvc, scm):
    tmp_dir.scm_gen({"foo": "foo"}, commit="initial")

    api.exp_save()

    api.exp_save("foo")
    with pytest.raises(
        ExperimentExistsError,
        match="Experiment conflicts with existing experiment 'foo'.",
    ):
        api.exp_save("foo")
    api.exp_save("foo", force=True)


def test_exp_show(tmp_dir, dvc, scm, exp_stage):  # noqa: F811
    exps = api.exp_show()

    assert len(exps) == 2
    assert isinstance(exps, list)
    assert isinstance(exps[0], dict)
    assert isinstance(exps[1], dict)
    # Postprocessing casting to float
    assert exps[0]["metrics.yaml:foo"] == 1.0
    # Postprocessing using `None` as fill value
    assert exps[0]["State"] is None
    # Postprocessing empty string as `None`
    assert exps[0]["Experiment"] is None
