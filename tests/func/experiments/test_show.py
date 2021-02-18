import logging
import os
from datetime import datetime

import pytest
from funcy import first

from dvc.main import main
from dvc.repo.experiments.base import ExpRefInfo


def test_show_simple(tmp_dir, scm, dvc, exp_stage):
    assert dvc.experiments.show()["workspace"] == {
        "baseline": {
            "metrics": {"metrics.yaml": {"foo": 1}},
            "params": {"params.yaml": {"foo": 1}},
            "queued": False,
            "timestamp": None,
        }
    }


@pytest.mark.parametrize("workspace", [True, False])
def test_show_experiment(tmp_dir, scm, dvc, exp_stage, workspace):
    baseline_rev = scm.get_rev()
    timestamp = datetime.fromtimestamp(
        scm.gitpython.repo.rev_parse(baseline_rev).committed_date
    )

    dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], tmp_dir=not workspace
    )
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


def test_show_queued(tmp_dir, scm, dvc, exp_stage):
    from dvc.repo.experiments.base import EXPS_STASH

    baseline_rev = scm.get_rev()

    dvc.experiments.run(exp_stage.addressing, params=["foo=2"], queue=True)
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

    dvc.experiments.run(exp_stage.addressing, params=["foo=3"], queue=True)
    exp_rev = dvc.experiments.scm.resolve_rev(f"{EXPS_STASH}@{{0}}")

    results = dvc.experiments.show()[new_rev]
    assert len(results) == 2
    exp = results[exp_rev]
    assert exp["queued"]
    assert exp["params"]["params.yaml"] == {"foo": 3}


@pytest.mark.parametrize("workspace", [True, False])
def test_show_checkpoint(
    tmp_dir, scm, dvc, checkpoint_stage, capsys, workspace
):
    baseline_rev = scm.get_rev()
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"], tmp_dir=not workspace
    )
    exp_rev = first(results)

    results = dvc.experiments.show()[baseline_rev]
    assert len(results) == checkpoint_stage.iterations + 1

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
            fs = "╓"
        elif i == len(checkpoints) - 1:
            name = rev[:7]
            fs = "╨"
        else:
            name = rev[:7]
            fs = "╟"
        assert f"{fs} {name}" in cap.out


@pytest.mark.parametrize("workspace", [True, False])
def test_show_checkpoint_branch(
    tmp_dir, scm, dvc, checkpoint_stage, capsys, workspace
):
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"], tmp_dir=not workspace
    )
    branch_rev = first(results)
    if not workspace:
        dvc.experiments.apply(branch_rev)

    results = dvc.experiments.run(
        checkpoint_stage.addressing,
        checkpoint_resume=branch_rev,
        tmp_dir=not workspace,
    )
    checkpoint_a = first(results)

    dvc.experiments.apply(branch_rev)
    results = dvc.experiments.run(
        checkpoint_stage.addressing,
        checkpoint_resume=branch_rev,
        params=["foo=100"],
        tmp_dir=not workspace,
    )
    checkpoint_b = first(results)

    capsys.readouterr()
    assert main(["exp", "show", "--no-pager"]) == 0
    cap = capsys.readouterr()

    for rev in (checkpoint_a, checkpoint_b):
        ref = dvc.experiments.get_branch_by_rev(rev)
        ref_info = ExpRefInfo.from_ref(ref)
        name = ref_info.name
        assert f"╓ {name}" in cap.out
    assert f"({branch_rev[:7]})" in cap.out


def test_show_filter(tmp_dir, scm, dvc, exp_stage, capsys):
    capsys.readouterr()
    assert (
        main(
            [
                "exp",
                "show",
                "--no-pager",
                "--no-timestamp",
                "--include-metrics=foo",
                "--include-params=foo",
            ]
        )
        == 0
    )
    cap = capsys.readouterr()

    div = "│" if os.name == "nt" else "┃"
    assert f"{div} foo {div} foo {div}" in cap.out

    assert (
        main(
            [
                "exp",
                "show",
                "--no-pager",
                "--no-timestamp",
                "--exclude-metrics=foo",
                "--exclude-params=foo",
            ]
        )
        == 0
    )
    cap = capsys.readouterr()

    assert f"{div} foo {div}" not in cap.out


def test_show_multiple_commits(tmp_dir, scm, dvc, exp_stage):
    from dvc.exceptions import InvalidArgumentError

    init_rev = scm.get_rev()
    tmp_dir.scm_gen("file", "file", "commit")
    next_rev = scm.get_rev()

    with pytest.raises(InvalidArgumentError):
        dvc.experiments.show(num=-1)

    expected = {"workspace", init_rev, next_rev}
    results = dvc.experiments.show(num=2)
    assert set(results.keys()) == expected

    expected = {"workspace"} | set(scm.branch_revs("master"))
    results = dvc.experiments.show(all_commits=True)
    assert set(results.keys()) == expected

    results = dvc.experiments.show(num=100)
    assert set(results.keys()) == expected


def test_show_sort(tmp_dir, scm, dvc, exp_stage, caplog):
    with caplog.at_level(logging.ERROR):
        assert main(["exp", "show", "--no-pager", "--sort-by=bar"]) != 0
        assert "Unknown sort column" in caplog.text

    with caplog.at_level(logging.ERROR):
        assert main(["exp", "show", "--no-pager", "--sort-by=foo"]) != 0
        assert "Ambiguous sort column" in caplog.text

    assert (
        main(["exp", "show", "--no-pager", "--sort-by=params.yaml:foo"]) == 0
    )

    assert (
        main(["exp", "show", "--no-pager", "--sort-by=metrics.yaml:foo"]) == 0
    )
