import logging
import os
import random
from datetime import datetime
from unittest.mock import ANY

import pytest
from funcy import first, get_in
from scmrepo.exceptions import SCMError

from dvc.cli import main
from dvc.repo.experiments.executor.base import BaseExecutor, ExecutorInfo, TaskStatus
from dvc.repo.experiments.queue.base import QueueEntry
from dvc.repo.experiments.refs import CELERY_STASH, ExpRefInfo
from dvc.repo.experiments.show import _CachedError
from dvc.repo.experiments.utils import EXEC_PID_DIR, EXEC_TMP_DIR, exp_refs_by_rev
from dvc.utils import relpath
from dvc.utils.serialize import YAMLFileCorruptedError

LOCK_CONTENTS = {
    "read": {
        "data/MNIST": [{"pid": 54062, "cmd": "dvc exp run"}],
    },
    "write": {
        "data/MNIST": {"pid": 54062, "cmd": "dvc exp run"},
    },
}


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


@pytest.mark.vscode
def test_show_branch_and_tag_name(tmp_dir, scm, dvc, exp_stage):
    with tmp_dir.branch("new/branch", new=True):
        tmp_dir.scm_gen("branch", "branch", "commit")
        branch_rev = scm.get_rev()

    result = dvc.experiments.show(all_branches=True)
    assert result[branch_rev]["baseline"]["data"]["name"] == "new/branch"

    scm.tag("new/tag")
    tag_rev = scm.get_rev()
    result = dvc.experiments.show(all_tags=True)
    assert result[tag_rev]["baseline"]["data"]["name"] == "new/tag"


@pytest.mark.vscode
def test_show_simple(tmp_dir, scm, dvc, exp_stage):
    assert dvc.experiments.show()["workspace"] == {
        "baseline": {
            "data": {
                "rev": "workspace",
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
                "status": "Success",
                "executor": None,
                "timestamp": None,
            }
        }
    }


@pytest.mark.vscode
@pytest.mark.parametrize("workspace", [True, False])
def test_show_experiment(tmp_dir, scm, dvc, exp_stage, workspace):
    baseline_rev = scm.get_rev()
    timestamp = datetime.fromtimestamp(
        scm.gitpython.repo.rev_parse(baseline_rev).committed_date
    )

    dvc.experiments.run(exp_stage.addressing, params=["foo=2"], tmp_dir=not workspace)
    results = dvc.experiments.show()

    expected_baseline = {
        "data": {
            "rev": ANY,
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
            "status": "Success",
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


@pytest.mark.vscode
def test_show_queued(tmp_dir, scm, dvc, exp_stage):
    baseline_rev = scm.get_rev()

    dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], queue=True, name="test_name"
    )
    exp_rev = dvc.experiments.scm.resolve_rev(f"{CELERY_STASH}@{{0}}")

    results = dvc.experiments.show()[baseline_rev]
    assert len(results) == 2
    exp = results[exp_rev]["data"]
    assert exp["name"] == "test_name"
    assert exp["status"] == "Queued"
    assert exp["params"]["params.yaml"] == {"data": {"foo": 2}}

    # test that only queued experiments for the current baseline are returned
    tmp_dir.gen("foo", "foo")
    scm.add(["foo"])
    scm.commit("new commit")
    new_rev = scm.get_rev()

    dvc.experiments.run(exp_stage.addressing, params=["foo=3"], queue=True)
    exp_rev = dvc.experiments.scm.resolve_rev(f"{CELERY_STASH}@{{0}}")

    results = dvc.experiments.show()[new_rev]
    assert len(results) == 2
    exp = results[exp_rev]["data"]
    assert exp["status"] == "Queued"
    assert exp["params"]["params.yaml"] == {"data": {"foo": 3}}


@pytest.mark.vscode
def test_show_failed_experiment(tmp_dir, scm, dvc, failed_exp_stage, test_queue):
    baseline_rev = scm.get_rev()
    timestamp = datetime.fromtimestamp(
        scm.gitpython.repo.rev_parse(baseline_rev).committed_date
    )

    dvc.experiments.run(
        failed_exp_stage.addressing, params=["foo=2"], queue=True, name="show-failed"
    )
    exp_rev = dvc.experiments.scm.resolve_rev(f"{CELERY_STASH}@{{0}}")
    dvc.experiments.run(run_all=True)
    test_queue.wait(["show-failed"])
    experiments = dvc.experiments.show()[baseline_rev]

    expected_baseline = {
        "data": {
            "rev": ANY,
            "deps": {
                "copy.py": {
                    "hash": ANY,
                    "size": ANY,
                    "nfiles": None,
                }
            },
            "metrics": {},
            "outs": {},
            "params": {"params.yaml": {"data": {"foo": 1}}},
            "status": "Success",
            "executor": None,
            "timestamp": timestamp,
            "name": "master",
        }
    }

    expected_failed = {
        "data": {
            "rev": ANY,
            "timestamp": ANY,
            "params": {"params.yaml": {"data": {"foo": 2}}},
            "deps": {"copy.py": {"hash": None, "size": None, "nfiles": None}},
            "metrics": {},
            "outs": {},
            "status": "Failed",
            "executor": None,
            "error": {
                "msg": "Experiment run failed.",
                "type": "",
            },
            "name": ANY,
        }
    }

    assert len(experiments) == 2
    for rev, exp in experiments.items():
        if rev == "baseline":
            assert exp == expected_baseline
        else:
            assert rev == exp_rev
            assert exp == expected_failed


@pytest.mark.vscode
@pytest.mark.parametrize("workspace", [True, False])
def test_show_checkpoint(tmp_dir, scm, dvc, checkpoint_stage, capsys, workspace):
    baseline_rev = scm.get_rev()
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"], tmp_dir=not workspace
    )
    exp_rev = first(results)

    results = dvc.experiments.show()[baseline_rev]
    # Assert 4 rows: baseline, 2 checkpoints, and final commit
    assert len(results) == checkpoint_stage.iterations + 2

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
            name = dvc.experiments.get_exact_name([rev])[rev]
            name = f"{rev[:7]} [{name}]"
            fs = "╓"
        elif i == len(checkpoints) - 1:
            name = rev[:7]
            fs = "╨"
        else:
            name = rev[:7]
            fs = "╟"
        assert f"{fs} {name}" in cap.out


@pytest.mark.vscode
@pytest.mark.parametrize("workspace", [True, False])
def test_show_checkpoint_branch(tmp_dir, scm, dvc, checkpoint_stage, capsys, workspace):
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
    copy_script,
):
    capsys.readouterr()

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


@pytest.mark.vscode
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

    assert main(["exp", "show", "--no-pager", "--sort-by=params.yaml:foo"]) == 0

    assert main(["exp", "show", "--no-pager", "--sort-by=metrics.yaml:foo"]) == 0


@pytest.mark.vscode
@pytest.mark.parametrize(
    "status, pid_exists",
    [
        (TaskStatus.RUNNING, True),
        (TaskStatus.RUNNING, False),
        (TaskStatus.FAILED, False),
    ],
)
def test_show_running_workspace(
    tmp_dir, scm, dvc, exp_stage, capsys, caplog, status, pid_exists, mocker
):
    from dvc.rwlock import RWLOCK_FILE

    pid_dir = os.path.join(dvc.tmp_dir, EXEC_TMP_DIR, EXEC_PID_DIR)
    lock_file = relpath(os.path.join(dvc.tmp_dir, RWLOCK_FILE), str(tmp_dir))
    info = make_executor_info(location=BaseExecutor.DEFAULT_LOCATION, status=status)
    pidfile = os.path.join(
        pid_dir,
        "workspace",
        f"workspace{BaseExecutor.INFOFILE_EXT}",
    )
    os.makedirs(os.path.dirname(pidfile), exist_ok=True)
    (tmp_dir / pidfile).dump_json(info.asdict())
    (tmp_dir / lock_file).dump_json(LOCK_CONTENTS)

    mocker.patch("psutil.pid_exists", return_value=pid_exists)

    assert dvc.experiments.show().get("workspace") == {
        "baseline": {
            "data": {
                "rev": "workspace",
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
                "status": (
                    "Running"
                    if status == TaskStatus.RUNNING and pid_exists
                    else "Success"
                ),
                "executor": (
                    info.location
                    if status == TaskStatus.RUNNING and pid_exists
                    else None
                ),
                "timestamp": None,
            }
        }
    }
    capsys.readouterr()
    assert main(["exp", "show", "--csv"]) == 0
    cap = capsys.readouterr()
    if status == TaskStatus.RUNNING:
        if pid_exists:
            assert "Running" in cap.out
            assert info.location in cap.out
        else:
            cmd = LOCK_CONTENTS["read"]["data/MNIST"][0]["cmd"]
            pid = LOCK_CONTENTS["read"]["data/MNIST"][0]["pid"]
            assert (
                f"Process '{cmd}' with (Pid {pid}), in RWLock-file "
                f"'{lock_file}' had been killed." in caplog.text
            )
            assert f"Delete corrupted RWLock-file '{lock_file}'" in caplog.text


def test_show_running_tempdir(tmp_dir, scm, dvc, exp_stage, mocker):
    baseline_rev = scm.get_rev()
    run_results = dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], tmp_dir=True
    )
    exp_rev = first(run_results)
    exp_ref = first(exp_refs_by_rev(scm, exp_rev))

    queue = dvc.experiments.tempdir_queue
    stash_rev = "abc123"
    entries = [
        QueueEntry(
            str(tmp_dir / ".dvc" / "tmp" / "foo"),
            str(tmp_dir / ".dvc" / "tmp" / "foo"),
            str(exp_ref),
            stash_rev,
            exp_ref.baseline_sha,
            None,
            exp_ref.name,
            None,
        )
    ]
    mocker.patch.object(
        dvc.experiments.tempdir_queue,
        "iter_active",
        return_value=entries,
    )
    info = make_executor_info(location=BaseExecutor.DEFAULT_LOCATION)
    pidfile = queue.get_infofile_path(stash_rev)
    os.makedirs(os.path.dirname(pidfile), exist_ok=True)
    (tmp_dir / pidfile).dump_json(info.asdict())
    mock_fetch = mocker.patch.object(
        dvc.experiments.tempdir_queue,
        "get_running_exps",
        return_value={exp_rev: info.asdict()},
    )

    results = dvc.experiments.show()
    mock_fetch.assert_has_calls(
        [mocker.call(True)],
    )
    exp_data = get_in(results, [baseline_rev, exp_rev, "data"])
    assert exp_data["status"] == "Running"
    assert exp_data["executor"] == info.location

    assert results["workspace"]["baseline"]["data"]["status"] == "Success"


def test_show_running_celery(tmp_dir, scm, dvc, exp_stage, mocker):
    baseline_rev = scm.get_rev()
    dvc.experiments.run(exp_stage.addressing, params=["foo=2"], queue=True)
    exp_rev = dvc.experiments.scm.resolve_rev(f"{CELERY_STASH}@{{0}}")

    queue = dvc.experiments.celery_queue
    entries = list(queue.iter_queued())
    mocker.patch.object(
        dvc.experiments.celery_queue,
        "iter_active",
        return_value=entries,
    )
    info = make_executor_info(location=BaseExecutor.DEFAULT_LOCATION)
    pidfile = queue.get_infofile_path(entries[0].stash_rev)
    os.makedirs(os.path.dirname(pidfile), exist_ok=True)
    (tmp_dir / pidfile).dump_json(info.asdict())

    results = dvc.experiments.show()
    exp_data = get_in(results, [baseline_rev, exp_rev, "data"])
    assert exp_data["status"] == "Running"
    assert exp_data["executor"] == info.location

    assert results["workspace"]["baseline"]["data"]["status"] == "Success"


def test_show_running_checkpoint(
    tmp_dir, scm, dvc, checkpoint_stage, mocker, test_queue
):
    from dvc.repo.experiments.executor.local import TempDirExecutor

    baseline_rev = scm.get_rev()
    dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"], queue=True, name="foo"
    )
    queue = dvc.experiments.celery_queue
    entries = list(queue.iter_queued())

    run_results = dvc.experiments.run(run_all=True)
    test_queue.wait(["foo"])
    checkpoint_rev = first(run_results)
    exp_ref = first(exp_refs_by_rev(scm, checkpoint_rev))

    mocker.patch.object(
        dvc.experiments.celery_queue,
        "iter_active",
        return_value=entries,
    )
    mocker.patch.object(
        dvc.experiments.celery_queue,
        "iter_failed",
        return_value=[],
    )
    pidfile = queue.get_infofile_path(entries[0].stash_rev)
    info = make_executor_info(
        git_url="foo.git",
        baseline_rev=baseline_rev,
        location=TempDirExecutor.DEFAULT_LOCATION,
        status=TaskStatus.RUNNING,
    )
    os.makedirs(os.path.dirname(pidfile), exist_ok=True)
    (tmp_dir / pidfile).dump_json(info.asdict())

    mocker.patch.object(BaseExecutor, "fetch_exps", return_value=[str(exp_ref)])

    results = dvc.experiments.show()

    checkpoint_res = get_in(results, [baseline_rev, checkpoint_rev, "data"])
    assert checkpoint_res["status"] == "Running"
    assert checkpoint_res["executor"] == info.location

    assert results["workspace"]["baseline"]["data"]["status"] == "Success"


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
    assert isinstance(get_in(result, paths), (_CachedError, YAMLFileCorruptedError))
    assert "YAML file structure is corrupted" in str(get_in(result, paths))


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

    # at least 1 second gap between these experiments to make sure
    # the previous experiment to be regarded as branch_base
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


def test_show_only_changed(tmp_dir, dvc, scm, capsys, copy_script):
    params_file = tmp_dir / "params.yaml"
    params_data = {
        "foo": 1,
        "goobar": 1,
    }
    (tmp_dir / params_file).dump(params_data)

    dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo", "goobar"],
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
    assert "goobar" in cap.out

    capsys.readouterr()
    assert main(["exp", "show", "--only-changed"]) == 0
    cap = capsys.readouterr()
    assert "goobar" not in cap.out

    capsys.readouterr()
    assert main(["exp", "show", "--only-changed", "--keep=.*bar"]) == 0
    cap = capsys.readouterr()
    assert "params.yaml:goobar" in cap.out
    assert "metrics.yaml:goobar" in cap.out


def test_show_parallel_coordinates(tmp_dir, dvc, scm, mocker, capsys, copy_script):
    from dvc.commands.experiments import show

    webbroser_open = mocker.patch("webbrowser.open")
    show_experiments = mocker.spy(show, "show_experiments")

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


@pytest.mark.vscode
def test_show_outs(tmp_dir, dvc, scm, erepo_dir, copy_script):
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
            "use_cache": True,
            "is_data_source": False,
        }
    }

    tmp_dir.dvc_gen("out_add", "foo", commit="dvc add output")

    outs = dvc.experiments.show()["workspace"]["baseline"]["data"]["outs"]
    assert outs == {
        "out": {
            "hash": ANY,
            "size": ANY,
            "nfiles": None,
            "use_cache": True,
            "is_data_source": False,
        },
        "out_add": {
            "hash": ANY,
            "size": ANY,
            "nfiles": None,
            "use_cache": True,
            "is_data_source": True,
        },
    }

    with erepo_dir.chdir():
        erepo_dir.dvc_gen("out", "out content", commit="create out")

    dvc.imp(os.fspath(erepo_dir), "out", "out_imported")

    outs = dvc.experiments.show()["workspace"]["baseline"]["data"]["outs"]
    assert outs == {
        "out": {
            "hash": ANY,
            "size": ANY,
            "nfiles": None,
            "use_cache": True,
            "is_data_source": False,
        },
        "out_add": {
            "hash": ANY,
            "size": ANY,
            "nfiles": None,
            "use_cache": True,
            "is_data_source": True,
        },
        "out_imported": {
            "hash": ANY,
            "size": ANY,
            "nfiles": None,
            "use_cache": True,
            "is_data_source": True,
        },
    }


def test_metrics_renaming(tmp_dir, dvc, scm, capsys, copy_script):
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


@pytest.mark.vscode
def test_show_queued_error(tmp_dir, scm, dvc, exp_stage, mocker):
    baseline_rev = scm.get_rev()

    dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], queue=True, name="test_name"
    )
    exp_rev_2 = dvc.experiments.scm.resolve_rev(f"{CELERY_STASH}@{{0}}")
    commit_2 = scm.resolve_commit(exp_rev_2)

    dvc.experiments.run(exp_stage.addressing, params=["foo=3"], queue=True)
    exp_rev_3 = dvc.experiments.scm.resolve_rev(f"{CELERY_STASH}@{{0}}")

    def resolve_commit(rev):
        if rev == exp_rev_3:
            raise SCMError
        return commit_2

    mocker.patch.object(
        scm,
        "resolve_commit",
        side_effect=mocker.MagicMock(side_effect=resolve_commit),
    )

    results = dvc.experiments.show()[baseline_rev]
    assert len(results) == 2
    exp_2 = results[exp_rev_2]["data"]
    assert exp_2["status"] == "Queued"
    assert exp_2["params"]["params.yaml"] == {"data": {"foo": 2}}


@pytest.mark.vscode
def test_show_completed_error(tmp_dir, scm, dvc, exp_stage, mocker):
    baseline_rev = scm.get_rev()

    result_2 = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp_rev_2 = first(result_2)
    commit_2 = scm.resolve_commit(exp_rev_2)
    result_3 = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    exp_rev_3 = first(result_3)

    def resolve_commit(rev):
        if rev == exp_rev_3:
            raise SCMError
        return commit_2

    mocker.patch.object(
        scm,
        "resolve_commit",
        side_effect=mocker.MagicMock(side_effect=resolve_commit),
    )
    experiments = dvc.experiments.show()[baseline_rev]
    assert len(experiments) == 2
    assert exp_rev_2 in experiments


@pytest.mark.vscode
def test_show_checkpoint_error(tmp_dir, scm, dvc, checkpoint_stage, mocker):
    baseline_rev = scm.get_rev()
    results = dvc.experiments.run(checkpoint_stage.addressing, params=["foo=2"])
    exp_rev = first(results)
    exp_ref = str(first(exp_refs_by_rev(scm, exp_rev)))

    results = dvc.experiments.show()[baseline_rev]
    # Assert 4 rows: baseline, 2 checkpoints, and final commit
    assert len(results) == checkpoint_stage.iterations + 2

    checkpoints = {}
    for rev in results:
        if rev != "baseline":
            checkpoints[rev] = scm.resolve_commit(rev)
    checkpoints[exp_ref] = scm.resolve_commit(exp_ref)
    checkpoints[baseline_rev] = scm.resolve_commit(baseline_rev)

    failed_rev = random.choice(list(checkpoints.keys()))

    def resolve_commit(rev):
        if rev == failed_rev:
            raise SCMError
        return checkpoints[rev]

    mocker.patch.object(
        scm,
        "resolve_commit",
        side_effect=mocker.MagicMock(side_effect=resolve_commit),
    )
    results = dvc.experiments.show(force=True)[baseline_rev]
    assert len(results) == 1


@pytest.mark.vscode
def test_show_baseline_error(tmp_dir, scm, dvc, exp_stage, mocker):
    baseline_rev = scm.get_rev()
    branch = scm.active_branch()

    result_2 = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp_rev_2 = first(result_2)
    commit_2 = scm.resolve_commit(exp_rev_2)

    def resolve_commit(rev):
        if rev == baseline_rev:
            raise SCMError
        return commit_2

    mocker.patch.object(
        scm,
        "resolve_commit",
        side_effect=mocker.MagicMock(side_effect=resolve_commit),
    )
    experiments = dvc.experiments.show()[baseline_rev]
    assert len(experiments) == 1
    assert experiments["baseline"]["data"] == {"name": branch}
    assert isinstance(experiments["baseline"]["error"], (SCMError, _CachedError))
