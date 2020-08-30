from datetime import datetime

from dvc.dvcfile import PIPELINE_FILE
from tests.func.test_repro_multistage import COPY_SCRIPT


def test_show_simple(tmp_dir, scm, dvc):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")
    dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        single_stage=True,
    )

    assert dvc.experiments.show()["workspace"] == {
        "baseline": {
            "metrics": {"metrics.yaml": {"foo": 1}},
            "params": {"params.yaml": {"foo": 1}},
            "queued": False,
            "timestamp": None,
        }
    }


def test_show_experiment(tmp_dir, scm, dvc):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")
    dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        name="foo",
    )
    scm.add(["copy.py", "params.yaml", "metrics.yaml", "dvc.yaml", "dvc.lock"])
    scm.commit("baseline")
    baseline_rev = scm.get_rev()
    timestamp = datetime.fromtimestamp(
        scm.repo.rev_parse(baseline_rev).committed_date
    )

    dvc.reproduce(PIPELINE_FILE, experiment=True, params=["foo=2"])
    results = dvc.experiments.show()

    expected_baseline = {
        "metrics": {"metrics.yaml": {"foo": 1}},
        "params": {"params.yaml": {"foo": 1}},
        "queued": False,
        "timestamp": timestamp,
        "name": "master",
    }
    expected_params = {"foo": 2}

    assert set(results.keys()) == {"workspace", baseline_rev}
    experiments = results[baseline_rev]
    assert len(experiments) == 2
    for rev, exp in experiments.items():
        if rev == "baseline":
            assert exp == expected_baseline
        else:
            assert exp["metrics"]["metrics.yaml"] == expected_params
            assert exp["params"]["params.yaml"] == expected_params


def test_show_queued(tmp_dir, scm, dvc):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")
    stage = dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        name="foo",
    )
    scm.add(["copy.py", "params.yaml", "metrics.yaml", "dvc.yaml", "dvc.lock"])
    scm.commit("baseline")
    baseline_rev = scm.get_rev()

    dvc.reproduce(
        stage.addressing, experiment=True, params=["foo=2"], queue=True
    )
    exp_rev = dvc.experiments.scm.resolve_rev("stash@{0}")

    results = dvc.experiments.show()[baseline_rev]
    assert len(results) == 2
    exp = results[exp_rev]
    assert exp["queued"]
    assert exp["params"]["params.yaml"] == {"foo": 2}

    # test that only queued experiments for the current baseline are returned
    tmp_dir.gen("foo", "foo")
    scm.add(["foo"])
    scm.commit("new commit")
    new_rev = scm.get_rev()

    dvc.reproduce(
        stage.addressing, experiment=True, params=["foo=3"], queue=True
    )
    exp_rev = dvc.experiments.scm.resolve_rev("stash@{0}")

    results = dvc.experiments.show()[new_rev]
    assert len(results) == 2
    exp = results[exp_rev]
    assert exp["queued"]
    assert exp["params"]["params.yaml"] == {"foo": 3}
