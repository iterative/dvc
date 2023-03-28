import pytest

from dvc import api
from dvc.repo.experiments.exceptions import ExperimentExistsError


def test_exp_save(tmp_dir, dvc, scm, mocker):
    tmp_dir.scm_gen({"foo": "foo"}, commit="initial")

    api.exp_save()

    api.exp_save("foo")
    with pytest.raises(
        ExperimentExistsError,
        match="Experiment conflicts with existing experiment 'foo'.",
    ):
        api.exp_save("foo")
    api.exp_save("foo", force=True)
