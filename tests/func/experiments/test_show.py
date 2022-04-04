import logging
import os
from datetime import datetime
from unittest.mock import ANY

import pytest
from funcy import first, get_in

from dvc.cli import main
from dvc.repo.experiments.base import EXPS_STASH, ExpRefInfo
from dvc.repo.experiments.executor.base import (
    EXEC_PID_DIR,
    EXEC_TMP_DIR,
    BaseExecutor,
    ExecutorInfo,
)
from dvc.repo.experiments.utils import exp_refs_by_rev
from dvc.utils.fs import makedirs
from dvc.utils.serialize import YAMLFileCorruptedError
from tests.func.test_repro_multistage import COPY_SCRIPT


def make_executor_info(**kwargs):
    # set default values for required info fields
    for key in (
        "git_url",
        "baseline_rev",
        "location",
        "root_dir",
        "dvc_dir",
    ):
        if key not in kwargs:
            kwargs[key] = ""
    return ExecutorInfo(**kwargs)


def test_show_simple(tmp_dir, scm, dvc, exp_stage):
    assert dvc.experiments.show()["workspace"] == {
        "baseline": {
            "data": {
                "deps": {
                    "copy.py": {
                        "hash": ANY,
                        "size": ANY,
                        "nfiles": None,
                    }
                },
                "metrics": {"metrics.yaml": {"data": {"foo": 1}}},
                "outs": {},
                "params": {"params.yaml": {"data": {"foo": 1}}},
                "queued": False,
                "running": False,
                "executor": None,
                "timestamp": None,
            }
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
        "data": {
            "deps": {
                "copy.py": {
                    "hash": ANY,
                    "size": ANY,
                    "nfiles": None,
                }
            },
            "metrics": {"metrics.yaml": {"data": {"foo": 1}}},
            "outs": {},
            "params": {"params.yaml": {"data": {"foo": 1}}},
            "queued": False,
            "running": False,
            "executor": None,
            "timestamp": timestamp,
            "name": "master",
        }
    }
    expected_params = {"data": {"foo": 2}}

    assert set(results.keys()) == {"workspace", baseline_rev}
    experiments = results[baseline_rev]
    assert len(experiments) == 2
    for rev, exp in experiments.items():
        if rev == "baseline":
            assert exp == expected_baseline
        else:
            assert exp["data"]["metrics"]["metrics.yaml"] == expected_params
            assert exp["data"]["params"]["params.yaml"] == expected_params


def test_show_queued(tmp_dir, scm, dvc, exp_stage):
    baseline_rev = scm.get_rev()

    dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], queue=True, name="test_name"
    )
    exp_rev = dvc.experiments.scm.resolve_rev(f"{EXPS_STASH}@{{0}}")

    results = dvc.experiments.show()[baseline_rev]
    assert len(results) == 2
    exp = results[exp_rev]["data"]
    assert exp["name"] == "test_name"
    assert exp["queued"]
    assert exp["params"]["params.yaml"] == {"data": {"foo": 2}}

    # test that only queued experiments for the current baseline are returned
    tmp_dir.gen("foo", "foo")
    scm.add(["foo"])
    scm.commit("new commit")
    new_rev = scm.get_rev()

    dvc.experiments.run(exp_stage.addressing, params=["foo=3"], queue=True)
    exp_rev = dvc.experiments.scm.resolve_rev(f"{EXPS_STASH}@{{0}}")

    results = dvc.experiments.show()[new_rev]
    assert len(results) == 2
    exp = results[exp_rev]["data"]
    assert exp["queued"]
    assert exp["params"]["params.yaml"] == {"data": {"foo": 3}}


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
            assert exp["data"]["checkpoint_tip"] == exp_rev

    capsys.readouterr()
    assert main(["exp", "show", "--no-pager"]) == 0
    cap = capsys.readouterr()

    for i, rev in enumerate(checkpoints):
        if i == 0:
            name = dvc.experiments.get_exact_name(rev)
            name = f"{rev[:7]} [{name}]"
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
        name = f"{rev[:7]} [{ref_info.name}]"
        assert f"╓ {name}" in cap.out
    assert f"({branch_rev[:7]})" in cap.out


def test_show_filter(
    tmp_dir,
    scm,
    dvc,
    capsys,
):
    capsys.readouterr()

    tmp_dir.gen("copy.py", COPY_SCRIPT)
    params_file = tmp_dir / "params.yaml"
    params_data = {
        "foo": 1,
        "bar": 1,
        "train/foo": 1,
        "train/bar": 1,
        "nested": {"foo": 1, "bar": 1},
    }
    (tmp_dir / params_file).dump(params_data)

    dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        name="copy-file",
        deps=["copy.py"],
    )
    scm.add(
        [
            "dvc.yaml",
            "dvc.lock",
            "copy.py",
            "params.yaml",
            "metrics.yaml",
            ".gitignore",
        ]
    )
    scm.commit("init")

    capsys.readouterr()
    assert main(["exp", "show", "--drop=.*foo"]) == 0
    cap = capsys.readouterr()
    for filtered in ["foo", "train/foo", "nested.foo"]:
        assert f"params.yaml:{filtered}" not in cap.out
        assert f"metrics.yaml:{filtered}" not in cap.out

    capsys.readouterr()
    assert main(["exp", "show", "--drop=.*foo", "--keep=.*train"]) == 0
    cap = capsys.readouterr()
    for filtered in ["foo", "nested.foo"]:
        assert f"params.yaml:{filtered}" not in cap.out
        assert f"metrics.yaml:{filtered}" not in cap.out
    assert "params.yaml:train/foo" in cap.out
    assert "metrics.yaml:train/foo" in cap.out

    capsys.readouterr()
    assert main(["exp", "show", "--drop=params.yaml:.*foo"]) == 0
    cap = capsys.readouterr()
    for filtered in ["foo", "train/foo", "nested.foo"]:
        assert f"params.yaml:{filtered}" not in cap.out
        assert f"metrics.yaml:{filtered}" in cap.out

    capsys.readouterr()
    assert main(["exp", "show", "--drop=Created"]) == 0
    cap = capsys.readouterr()
    assert "Created" not in cap.out

    capsys.readouterr()
    assert main(["exp", "show", "--drop=Created|Experiment"]) == 0
    cap = capsys.readouterr()
    assert "Created" not in cap.out
    assert "Experiment" not in cap.out


def test_show_multiple_commits(tmp_dir, scm, dvc, exp_stage):
    init_rev = scm.get_rev()
    tmp_dir.scm_gen("file", "file", "commit")
    next_rev = scm.get_rev()

    dvc.experiments.show(num=-2)

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


def test_show_running_workspace(tmp_dir, scm, dvc, exp_stage, capsys):
    pid_dir = os.path.join(dvc.tmp_dir, EXEC_TMP_DIR, EXEC_PID_DIR)
    info = make_executor_info(location=BaseExecutor.DEFAULT_LOCATION)
    pidfile = os.path.join(
        pid_dir,
        "workspace",
        f"workspace{BaseExecutor.INFOFILE_EXT}",
    )
    makedirs(os.path.dirname(pidfile), True)
    (tmp_dir / pidfile).dump_json(info.asdict())

    assert dvc.experiments.show()["workspace"] == {
        "baseline": {
            "data": {
                "deps": {
                    "copy.py": {
                        "hash": ANY,
                        "size": ANY,
                        "nfiles": None,
                    }
                },
                "metrics": {"metrics.yaml": {"data": {"foo": 1}}},
                "params": {"params.yaml": {"data": {"foo": 1}}},
                "outs": {},
                "queued": False,
                "running": True,
                "executor": info.location,
                "timestamp": None,
            }
        }
    }

    capsys.readouterr()
    assert main(["exp", "show", "--csv"]) == 0
    cap = capsys.readouterr()
    assert "Running" in cap.out
    assert info.location in cap.out


def test_show_running_executor(tmp_dir, scm, dvc, exp_stage):
    baseline_rev = scm.get_rev()
    dvc.experiments.run(exp_stage.addressing, params=["foo=2"], queue=True)
    exp_rev = dvc.experiments.scm.resolve_rev(f"{EXPS_STASH}@{{0}}")

    pid_dir = os.path.join(dvc.tmp_dir, EXEC_TMP_DIR, EXEC_PID_DIR)
    info = make_executor_info(location=BaseExecutor.DEFAULT_LOCATION)
    pidfile = os.path.join(
        pid_dir,
        exp_rev,
        f"{exp_rev}{BaseExecutor.INFOFILE_EXT}",
    )
    makedirs(os.path.dirname(pidfile), True)
    (tmp_dir / pidfile).dump_json(info.asdict())

    results = dvc.experiments.show()
    exp_data = get_in(results, [baseline_rev, exp_rev, "data"])
    assert not exp_data["queued"]
    assert exp_data["running"]
    assert exp_data["executor"] == info.location

    assert not results["workspace"]["baseline"]["data"]["running"]


@pytest.mark.parametrize("workspace", [True, False])
def test_show_running_checkpoint(
    tmp_dir, scm, dvc, checkpoint_stage, workspace, mocker
):
    from dvc.repo.experiments.base import EXEC_BASELINE, EXEC_BRANCH
    from dvc.repo.experiments.executor.local import TempDirExecutor

    baseline_rev = scm.get_rev()
    dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"], queue=True
    )
    stash_rev = dvc.experiments.scm.resolve_rev(f"{EXPS_STASH}@{{0}}")

    run_results = dvc.experiments.run(run_all=True)
    checkpoint_rev = first(run_results)
    exp_ref = first(exp_refs_by_rev(scm, checkpoint_rev))

    pid_dir = os.path.join(dvc.tmp_dir, EXEC_TMP_DIR, EXEC_PID_DIR)
    executor = (
        BaseExecutor.DEFAULT_LOCATION
        if workspace
        else TempDirExecutor.DEFAULT_LOCATION
    )
    info = make_executor_info(
        git_url="foo.git",
        baseline_rev=baseline_rev,
        location=executor,
    )
    rev = "workspace" if workspace else stash_rev
    pidfile = os.path.join(pid_dir, f"{rev}{BaseExecutor.INFOFILE_EXT}")
    makedirs(os.path.dirname(pidfile), True)
    (tmp_dir / pidfile).dump_json(info.asdict())

    mocker.patch.object(
        BaseExecutor, "fetch_exps", return_value=[str(exp_ref)]
    )
    if workspace:
        scm.set_ref(EXEC_BRANCH, str(exp_ref), symbolic=True)
        scm.set_ref(EXEC_BASELINE, str(baseline_rev))
        scm.checkout(str(exp_ref))

    results = dvc.experiments.show()

    checkpoint_res = get_in(results, [baseline_rev, checkpoint_rev, "data"])
    assert checkpoint_res["running"]
    assert checkpoint_res["executor"] == info.location

    assert not results["workspace"]["baseline"]["data"]["running"]


def test_show_with_broken_repo(tmp_dir, scm, dvc, exp_stage, caplog):
    baseline_rev = scm.get_rev()
    exp1 = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp2 = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])

    with open("dvc.yaml", "a", encoding="utf-8") as fd:
        fd.write("breaking the yaml!")

    result = dvc.experiments.show()
    rev1 = first(exp1)
    rev2 = first(exp2)

    baseline = result[baseline_rev]

    paths = ["data", "params", "params.yaml"]
    assert get_in(baseline[rev1], paths) == {"data": {"foo": 2}}
    assert get_in(baseline[rev2], paths) == {"data": {"foo": 3}}

    paths = ["workspace", "baseline", "error"]
    assert isinstance(get_in(result, paths), YAMLFileCorruptedError)


def test_show_csv(tmp_dir, scm, dvc, exp_stage, capsys):
    import time

    baseline_rev = scm.get_rev()

    def _get_rev_isotimestamp(rev):
        return datetime.fromtimestamp(
            scm.gitpython.repo.rev_parse(rev).committed_date
        ).isoformat()

    result1 = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    rev1 = first(result1)
    ref_info1 = first(exp_refs_by_rev(scm, rev1))
    time.sleep(1)
    result2 = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    rev2 = first(result2)
    ref_info2 = first(exp_refs_by_rev(scm, rev2))

    capsys.readouterr()
    assert main(["exp", "show", "--csv"]) == 0
    cap = capsys.readouterr()
    data_dep = first(x for x in dvc.index.deps if "copy.py" in x.fspath)
    data_hash = data_dep.hash_info.value[:7]
    assert "Experiment,rev,typ,Created,parent" in cap.out
    assert "metrics.yaml:foo,params.yaml:foo,copy.py" in cap.out
    assert f",workspace,baseline,,,3,3,{data_hash}" in cap.out
    assert (
        "master,{},baseline,{},,1,1,{}".format(
            baseline_rev[:7], _get_rev_isotimestamp(baseline_rev), data_hash
        )
        in cap.out
    )
    assert (
        "{},{},branch_base,{},,2,2,{}".format(
            ref_info1.name, rev1[:7], _get_rev_isotimestamp(rev1), data_hash
        )
        in cap.out
    )
    assert (
        "{},{},branch_commit,{},,3,3,{}".format(
            ref_info2.name, rev2[:7], _get_rev_isotimestamp(rev2), data_hash
        )
        in cap.out
    )


def test_show_only_changed(tmp_dir, dvc, scm, capsys):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    params_file = tmp_dir / "params.yaml"
    params_data = {
        "foo": 1,
        "bar": 1,
    }
    (tmp_dir / params_file).dump(params_data)

    dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo", "bar"],
        name="copy-file",
        deps=["copy.py"],
    )
    scm.add(
        [
            "dvc.yaml",
            "dvc.lock",
            "copy.py",
            "params.yaml",
            "metrics.yaml",
            ".gitignore",
        ]
    )
    scm.commit("init")

    dvc.experiments.run(params=["foo=2"])

    capsys.readouterr()
    assert main(["exp", "show"]) == 0
    cap = capsys.readouterr()
    assert "bar" in cap.out

    capsys.readouterr()
    assert main(["exp", "show", "--only-changed"]) == 0
    cap = capsys.readouterr()
    assert "bar" not in cap.out

    capsys.readouterr()
    assert main(["exp", "show", "--only-changed", "--keep=.*bar"]) == 0
    cap = capsys.readouterr()
    assert "params.yaml:bar" in cap.out
    assert "metrics.yaml:bar" in cap.out


def test_show_parallel_coordinates(tmp_dir, dvc, scm, mocker, capsys):
    from dvc.commands.experiments import show

    webbroser_open = mocker.patch("webbrowser.open")
    show_experiments = mocker.spy(show, "show_experiments")

    tmp_dir.gen("copy.py", COPY_SCRIPT)
    params_file = tmp_dir / "params.yaml"
    params_data = {
        "foo": 1,
        "bar": 1,
    }
    (tmp_dir / params_file).dump(params_data)

    dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo", "bar"],
        name="copy-file",
        deps=["copy.py"],
    )
    scm.add(
        [
            "dvc.yaml",
            "dvc.lock",
            "copy.py",
            "params.yaml",
            "metrics.yaml",
            ".gitignore",
        ]
    )
    scm.commit("init")

    dvc.experiments.run(params=["foo=2"])

    assert main(["exp", "show", "--pcp"]) == 0
    kwargs = show_experiments.call_args[1]

    html_text = (tmp_dir / "dvc_plots" / "index.html").read_text()
    assert all(rev in html_text for rev in ["workspace", "master"])
    assert "[exp-" not in html_text

    assert '{"label": "metrics.yaml:foo", "values": [2.0, 1.0]}' in html_text
    assert '{"label": "params.yaml:foo", "values": [2.0, 1.0]}' in html_text
    assert '"line": {"color": [1, 0]' in html_text
    assert '"label": "metrics.yaml:bar"' not in html_text
    assert '"label": "Created"' not in html_text

    assert main(["exp", "show", "--pcp", "--sort-by", "metrics.yaml:foo"]) == 0
    kwargs = show_experiments.call_args[1]

    html_text = (tmp_dir / "dvc_plots" / "index.html").read_text()
    assert '"line": {"color": [2.0, 1.0]' in html_text

    assert main(["exp", "show", "--pcp", "--out", "experiments"]) == 0
    kwargs = show_experiments.call_args[1]

    assert kwargs["out"] == "experiments"
    assert (tmp_dir / "experiments" / "index.html").exists()

    assert main(["exp", "show", "--pcp", "--open"]) == 0

    webbroser_open.assert_called()

    params_data = {"foo": 1, "bar": 1, "foobar": 2}
    (tmp_dir / params_file).dump(params_data)
    assert main(["exp", "show", "--pcp"]) == 0
    html_text = (tmp_dir / "dvc_plots" / "index.html").read_text()
    assert '{"label": "foobar", "values": [2.0, null, null]}' in html_text

    assert main(["exp", "show", "--pcp", "--drop", "foobar"]) == 0
    html_text = (tmp_dir / "dvc_plots" / "index.html").read_text()
    assert '"label": "Created"' not in html_text
    assert '"label": "foobar"' not in html_text

    assert main(["exp", "show", "--pcp", "--drop", "Experiment"]) == 0
    html_text = (tmp_dir / "dvc_plots" / "index.html").read_text()
    assert '"label": "Experiment"' not in html_text


def test_show_outs(tmp_dir, dvc, scm):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    params_file = tmp_dir / "params.yaml"
    params_data = {
        "foo": 1,
        "bar": 1,
    }
    (tmp_dir / params_file).dump(params_data)

    dvc.run(
        cmd="python copy.py params.yaml metrics.yaml && echo out > out",
        metrics_no_cache=["metrics.yaml"],
        params=["foo", "bar"],
        name="copy-file",
        deps=["copy.py"],
        outs=["out"],
    )

    scm.commit("init")

    outs = dvc.experiments.show()["workspace"]["baseline"]["data"]["outs"]
    assert outs == {
        "out": {
            "hash": ANY,
            "size": ANY,
            "nfiles": None,
        }
    }


def test_metrics_renaming(tmp_dir, dvc, scm, capsys):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    params_file = tmp_dir / "params.yaml"
    params_data = {
        "foo": 1,
    }
    (tmp_dir / params_file).dump(params_data)

    dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        name="copy-file",
        deps=["copy.py"],
    )
    scm.add(
        [
            "dvc.yaml",
            "dvc.lock",
            "copy.py",
            "params.yaml",
            "metrics.yaml",
            ".gitignore",
        ]
    )

    scm.commit("metrics.yaml")
    metrics_rev = scm.get_rev()

    dvc.run(
        cmd="python copy.py params.yaml scores.yaml",
        metrics_no_cache=["scores.yaml"],
        params=["foo"],
        name="copy-file",
        deps=["copy.py"],
    )
    scm.add(
        [
            "dvc.yaml",
            "dvc.lock",
            "params.yaml",
            "scores.yaml",
        ]
    )
    scm.commit("scores.yaml")
    scores_rev = scm.get_rev()

    capsys.readouterr()
    assert main(["exp", "show", "--csv", "-A"]) == 0
    cap = capsys.readouterr()

    def _get_rev_isotimestamp(rev):
        return datetime.fromtimestamp(
            scm.gitpython.repo.rev_parse(rev).committed_date
        ).isoformat()

    assert (
        "master,{},baseline,{},,1,,1".format(
            scores_rev[:7], _get_rev_isotimestamp(scores_rev)
        )
        in cap.out
    )
    assert (
        ",{},baseline,{},,,1,1".format(
            metrics_rev[:7], _get_rev_isotimestamp(metrics_rev)
        )
        in cap.out
    )


def test_show_sorted_deps(tmp_dir, dvc, scm, capsys):
    tmp_dir.gen("a", "a")
    tmp_dir.gen("b", "b")
    tmp_dir.gen("c", "c")
    tmp_dir.gen("z", "z")

    dvc.run(
        cmd="echo foo",
        name="deps",
        deps=["a", "b", "z", "c"],
    )

    capsys.readouterr()
    assert main(["exp", "show", "--csv"]) == 0
    cap = capsys.readouterr()
    assert "a,b,c,z" in cap.out
