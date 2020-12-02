from datetime import datetime

from funcy import first

from dvc.dvcfile import PIPELINE_FILE
from dvc.main import main
from dvc.repo.experiments.base import ExpRefInfo
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

    dvc.experiments.run(PIPELINE_FILE, params=["foo=2"])
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
    from dvc.repo.experiments.base import EXPS_STASH

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

    dvc.experiments.run(stage.addressing, params=["foo=2"], queue=True)
    exp_rev = dvc.experiments.scm.resolve_rev(f"{EXPS_STASH}@{{0}}")

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

    dvc.experiments.run(stage.addressing, params=["foo=3"], queue=True)
    exp_rev = dvc.experiments.scm.resolve_rev(f"{EXPS_STASH}@{{0}}")

    results = dvc.experiments.show()[new_rev]
    assert len(results) == 2
    exp = results[exp_rev]
    assert exp["queued"]
    assert exp["params"]["params.yaml"] == {"foo": 3}


def test_show_checkpoint(tmp_dir, scm, dvc, checkpoint_stage, capsys):
    baseline_rev = scm.get_rev()
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"]
    )
    exp_rev = first(results)

    results = dvc.experiments.show()[baseline_rev]
    assert len(results) == 6

    checkpoints = []
    for rev, exp in results.items():
        if rev != "baseline":
            checkpoints.append(rev)
            assert exp["checkpoint_tip"] == exp_rev

    capsys.readouterr()
    assert main(["exp", "show", "--no-pager"]) == 0
    cap = capsys.readouterr()

    for i, rev in enumerate(checkpoints):
        if i == 0:
            name = dvc.experiments.get_exact_name(rev)
            tree = "╓"
        elif i == len(checkpoints) - 1:
            name = rev[:7]
            tree = "╨"
        else:
            name = rev[:7]
            tree = "╟"
        assert f"{tree} {name}" in cap.out


def test_show_checkpoint_branch(tmp_dir, scm, dvc, checkpoint_stage, capsys):
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"]
    )
    branch_rev = first(results)

    results = dvc.experiments.run(
        checkpoint_stage.addressing, checkpoint_resume=branch_rev
    )
    checkpoint_a = first(results)

    results = dvc.experiments.run(
        checkpoint_stage.addressing,
        checkpoint_resume=branch_rev,
        params=["foo=100"],
    )
    checkpoint_b = first(results)

    capsys.readouterr()
    assert main(["exp", "show", "--no-pager"]) == 0
    cap = capsys.readouterr()

    for rev in (checkpoint_a, checkpoint_b):
        ref = dvc.experiments.get_branch_containing(rev)
        ref_info = ExpRefInfo.from_ref(ref)
        name = ref_info.name
        assert f"╓ {name}" in cap.out
    assert f"({branch_rev[:7]})" in cap.out
