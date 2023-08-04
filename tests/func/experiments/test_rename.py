def test_rename_experiment_by_name(scm, dvc, exp_stage):
    dvc.experiments.run(exp_stage.addressing, name="test-name", params=["foo=1"])
    assert dvc.experiments.rename("new-name", "test-name") == ["new-name"]


def test_rename_experiment_by_rev(scm, dvc, exp_stage):
    baseline = scm.get_rev()
    dvc.experiments.run(exp_stage.addressing, name="test-name", params=["foo=1"])
    assert dvc.experiments.rename("new-name", rev=baseline) == ["new-name"]
