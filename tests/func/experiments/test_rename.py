import pytest

from dvc.repo.experiments.exceptions import UnresolvedExpNamesError
from dvc.repo.experiments.utils import exp_refs_by_names


def test_rename_experiment_by_name(scm, dvc, exp_stage):
    dvc.experiments.run(exp_stage.addressing, name="test-name", params=["foo=1"])
    old_ref = exp_refs_by_names(scm, {"test-name"})
    dvc.experiments.rename("new-name", "test-name")
    new_ref = exp_refs_by_names(scm, {"new-name"})
    assert scm.get_ref(str(old_ref["test-name"][0])) is None
    assert scm.get_ref(str(new_ref["new-name"][0])) is not None
    with pytest.raises(UnresolvedExpNamesError):
        dvc.experiments.rename("new-name", "foo")


def test_same_name(dvc, exp_stage):
    dvc.experiments.run(exp_stage.addressing, name="same-name", params=["foo=1"])
    assert dvc.experiments.rename("same-name", "same-name") is None
