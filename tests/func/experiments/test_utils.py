from funcy import first


def test_generate_random_exp_name(tmp_dir, dvc, scm, exp_stage, mocker):
    mocked_generator = mocker.MagicMock()
    mocked_generator.choice.side_effect = [
        0,
        0,
        0,
        0,
        1,
        1,
        0,
        0,
    ]
    mocker.patch(
        "dvc.repo.experiments.utils.random.Random", return_value=mocked_generator
    )

    ref = first(dvc.experiments.run(exp_stage.addressing, params=["foo=1"]))
    assert dvc.experiments.get_exact_name([ref])[ref] == "0-0"

    # Causes 1 retry
    ref = first(dvc.experiments.run(exp_stage.addressing, params=["foo=2"]))
    assert dvc.experiments.get_exact_name([ref])[ref] == "1-1"

    tmp_dir.scm_gen({"foo": "bar"}, commit="foo")
    # Can use same name because of different baseline_rev
    ref = first(dvc.experiments.run(exp_stage.addressing, params=["foo=1"]))
    assert dvc.experiments.get_exact_name([ref])[ref] == "0-0"
