from tests.func.test_repro_multistage import COPY_SCRIPT


def test_new_simple(tmp_dir, scm, dvc, mocker):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")
    stage = dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        name="copy-file",
    )
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("init")

    tmp_dir.gen("params.yaml", "foo: 2")

    new_mock = mocker.spy(dvc.experiments, "new")
    dvc.reproduce(stage.addressing, experiment=True)

    new_mock.assert_called_once()
    assert (
        tmp_dir / ".dvc" / "experiments" / "metrics.yaml"
    ).read_text() == "foo: 2"


def test_update_with_pull(tmp_dir, scm, dvc, mocker):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")
    stage = dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        name="copy-file",
    )
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("init")
    expected_revs = [scm.get_rev()]

    tmp_dir.gen("params.yaml", "foo: 2")
    dvc.reproduce(stage.addressing, experiment=True)
    scm.add(["dvc.yaml", "dvc.lock", "params.yaml", "metrics.yaml"])
    scm.commit("promote experiment")
    expected_revs.append(scm.get_rev())

    tmp_dir.gen("params.yaml", "foo: 3")
    dvc.reproduce(stage.addressing, experiment=True)

    exp_scm = dvc.experiments.scm
    for rev in expected_revs:
        assert exp_scm.has_rev(rev)
