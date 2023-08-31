import pytest

from dvc.repo.experiments.exceptions import UnresolvedExpNamesError


def test_rename_experiment_by_name(scm, dvc, exp_stage):
    dvc.experiments.run(exp_stage.addressing, name="test-name", params=["foo=1"])
    assert dvc.experiments.rename("new-name", "test-name") == ["test-name"]
    with pytest.raises(UnresolvedExpNamesError):
        dvc.experiments.rename("new-name", "foo")
