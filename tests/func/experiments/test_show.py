import logging
import os
from datetime import datetime
from unittest.mock import ANY

import pytest
from funcy import first
from scmrepo.exceptions import SCMError

from dvc.cli import main
from dvc.repo.experiments.executor.base import BaseExecutor, ExecutorInfo, TaskStatus
from dvc.repo.experiments.refs import CELERY_STASH
from dvc.repo.experiments.utils import EXEC_PID_DIR, EXEC_TMP_DIR, exp_refs_by_rev
from dvc.utils import relpath

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


def make_executor(local=None, **kwargs):
    if local:
        local_executor = {
            "root": ANY,
            "log": ANY,
            "pid": ANY,
            "returncode": ANY,
            "task_id": ANY,
        }
        local_executor.update(local)
    else:
        local_executor = ANY
    data = {"state": ANY, "local": local_executor, "name": ANY}
    data.update(kwargs)
    return data


def make_data(params=None, **kwargs):
    params = {"data": params or {"foo": 1}}
    data = {
        "rev": ANY,
        "deps": {"copy.py": {"hash": ANY, "size": ANY, "nfiles": None}},
        "metrics": {"metrics.yaml": params},
        "outs": {},
        "params": {"params.yaml": params},
        "timestamp": ANY,
        "meta": ANY,
    }
    data.update(kwargs)
    return data


@pytest.mark.vscode
def test_show_branch_and_tag_name(tmp_dir, scm, dvc, exp_stage):
    with tmp_dir.branch("new/branch", new=True):
        tmp_dir.scm_gen("branch", "branch", "commit")

    result = dvc.experiments.show(all_branches=True)
    expected = [None, "master", "new/branch"]
    assert [exp.name for exp in result] == expected

    scm.tag("new/tag")
    tag_rev = scm.get_rev()
    with scm.detach_head(tag_rev):
        result = dvc.experiments.show(all_tags=True)
    expected = [None, "new/tag"]
    assert [exp.name for exp in result] == expected


@pytest.mark.vscode
def test_show_simple(tmp_dir, scm, dvc, exp_stage):
    assert dvc.experiments.show()[0].dumpd() == {
        "rev": "workspace",
        "name": None,
        "data": make_data(rev="workspace"),
        "error": None,
        "experiments": None,
    }


@pytest.mark.vscode
@pytest.mark.parametrize("workspace", [True, False])
def test_show_experiment(tmp_dir, scm, dvc, exp_stage, workspace):
    baseline_rev = scm.get_rev()
    timestamp = datetime.fromtimestamp(  # noqa: DTZ006
        scm.gitpython.repo.rev_parse(baseline_rev).committed_date
    )

    exp_rev = first(
        dvc.experiments.run(
            exp_stage.addressing, params=["foo=2"], tmp_dir=not workspace
        )
    )
    results = dvc.experiments.show()
    assert results[1].dumpd() == {
        "rev": baseline_rev,
        "name": "master",
        "data": make_data(rev=baseline_rev, timestamp=timestamp),
        "error": None,
        "experiments": [
            {
                "revs": [
                    {
                        "rev": exp_rev,
                        "name": ANY,
                        "data": make_data(rev=exp_rev, params={"foo": 2}),
                        "error": None,
                        "experiments": None,
                    }
                ],
                "executor": None,
                "name": ANY,
            }
        ],
    }


@pytest.mark.vscode
def test_show_queued(tmp_dir, scm, dvc, exp_stage):
    baseline_rev = scm.get_rev()

    dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], queue=True, name="test_name"
    )
    exp_rev = dvc.experiments.scm.resolve_rev(f"{CELERY_STASH}@{{0}}")

    results = dvc.experiments.show()
    assert results[1].dumpd() == {
        "rev": baseline_rev,
        "name": "master",
        "data": make_data(rev=baseline_rev),
        "error": None,
        "experiments": [
            {
                "revs": [
                    {
                        "rev": exp_rev,
                        "name": "test_name",
                        "data": make_data(rev=exp_rev, params={"foo": 2}, metrics=ANY),
                        "error": None,
                        "experiments": None,
                    }
                ],
                "executor": make_executor(state="queued"),
                "name": "test_name",
            }
        ],
    }

    # test that only queued experiments for the current baseline are returned
    tmp_dir.gen("foo", "foo")
    scm.add(["foo"])
    scm.commit("new commit")
    new_rev = scm.get_rev()

    dvc.experiments.run(exp_stage.addressing, params=["foo=3"], queue=True)
    exp_rev = dvc.experiments.scm.resolve_rev(f"{CELERY_STASH}@{{0}}")

    results = dvc.experiments.show()
    assert results[1].dumpd() == {
        "rev": new_rev,
        "name": "master",
        "data": make_data(rev=new_rev),
        "error": None,
        "experiments": [
            {
                "revs": [
                    {
                        "rev": exp_rev,
                        "name": ANY,
                        "data": make_data(rev=exp_rev, params={"foo": 3}, metrics=ANY),
                        "error": None,
                        "experiments": None,
                    }
                ],
                "executor": make_executor(state="queued"),
                "name": ANY,
            }
        ],
    }


@pytest.mark.vscode
def test_show_failed_experiment(tmp_dir, scm, dvc, failed_exp_stage, test_queue):
    baseline_rev = scm.get_rev()
    dvc.experiments.run(failed_exp_stage.addressing, params=["foo=2"], queue=True)
    exp_rev = dvc.experiments.scm.resolve_rev(f"{CELERY_STASH}@{{0}}")
    dvc.experiments.run(run_all=True)

    results = dvc.experiments.show()
    assert results[1].dumpd() == {
        "rev": baseline_rev,
        "name": "master",
        "data": make_data(rev=baseline_rev, metrics=ANY),
        "error": None,
        "experiments": [
            {
                "revs": [
                    {
                        "rev": exp_rev,
                        "name": ANY,
                        "data": make_data(rev=exp_rev, params={"foo": 2}, metrics=ANY),
                        "error": {"msg": "Experiment run failed", "type": ANY},
                        "experiments": None,
                    }
                ],
                "executor": make_executor(state="failed", local={"returncode": 255}),
                "name": ANY,
            }
        ],
    }


def test_show_filter(tmp_dir, scm, dvc, capsys, copy_script):
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

    expected = ["workspace", next_rev, init_rev]
    results = dvc.experiments.show(num=2)
    assert [exp.rev for exp in results] == expected

    expected = ["workspace", *scm.branch_revs("master")]
    results = dvc.experiments.show(all_commits=True)
    assert [exp.rev for exp in results] == expected

    results = dvc.experiments.show(num=100)
    assert [exp.rev for exp in results] == expected


def test_show_sort(tmp_dir, scm, dvc, exp_stage, caplog):
    dvc.experiments.run(exp_stage.addressing, params=["foo=2"])

    with caplog.at_level(logging.ERROR):
        assert main(["exp", "show", "--no-pager", "--sort-by=bar"]) != 0
        assert "Unknown sort column" in caplog.text

    with caplog.at_level(logging.ERROR):
        assert main(["exp", "show", "--no-pager", "--sort-by=foo"]) != 0
        assert "Ambiguous sort column" in caplog.text

    assert main(["exp", "show", "--no-pager", "--sort-by=params.yaml:foo"]) == 0

    assert main(["exp", "show", "--no-pager", "--sort-by=metrics.yaml:foo"]) == 0


def test_show_sort_metric_sep(tmp_dir, scm, dvc, caplog):
    metrics_path = tmp_dir / "metrics:1.json"
    metrics_path.write_text('{"my::metric": 1, "other_metric": 0.5}')
    metrics_path = tmp_dir / "metrics:2.json"
    metrics_path.write_text('{"my::metric": 2}')
    dvcyaml_path = tmp_dir / "dvc.yaml"
    dvcyaml_path.write_text("metrics: ['metrics:1.json', 'metrics:2.json']")
    dvc.experiments.save()
    assert (
        main(["exp", "show", "--no-pager", "--sort-by=metrics:1.json:my::metric"]) == 0
    )
    assert main(["exp", "show", "--no-pager", "--sort-by=:other_metric"]) == 0


@pytest.mark.vscode
@pytest.mark.parametrize(
    "status, pid_exists",
    [
        (TaskStatus.RUNNING, True),
        (TaskStatus.RUNNING, False),
        (TaskStatus.FAILED, False),
    ],
)
def test_show_running(
    tmp_dir, scm, dvc, exp_stage, capsys, caplog, status, pid_exists, mocker
):
    from dvc.rwlock import RWLOCK_FILE
    from dvc_task.proc.process import ProcessInfo

    baseline_rev = scm.get_rev()
    pid_dir = os.path.join(dvc.tmp_dir, EXEC_TMP_DIR, EXEC_PID_DIR)
    lock_file = relpath(os.path.join(dvc.tmp_dir, RWLOCK_FILE), str(tmp_dir))
    info = make_executor_info(
        location=BaseExecutor.DEFAULT_LOCATION,
        status=status,
        baseline_rev=baseline_rev,
    )
    pidfile = os.path.join(
        pid_dir,
        "workspace",
        f"workspace{BaseExecutor.INFOFILE_EXT}",
    )
    os.makedirs(os.path.dirname(pidfile), exist_ok=True)
    (tmp_dir / pidfile).dump_json(info.asdict())
    (tmp_dir / lock_file).dump_json(LOCK_CONTENTS)

    mocker.patch.object(ProcessInfo, "load", return_value=mocker.Mock(pid=123))
    mocker.patch("psutil.pid_exists", return_value=pid_exists)

    tempdir_active = mocker.spy(dvc.experiments.tempdir_queue, "collect_active_data")
    celery_active = mocker.spy(dvc.experiments.celery_queue, "collect_active_data")
    results = dvc.experiments.show()
    assert results[1].dumpd() == {
        "rev": ANY,
        "name": "master",
        "data": make_data(),
        "error": None,
        "experiments": [
            {
                "revs": ANY,
                "executor": make_executor(state="running"),
                "name": ANY,
            }
        ]
        if pid_exists
        else None,
    }
    tempdir_active.assert_called_once()
    celery_active.assert_called_once()


def test_show_with_broken_repo(tmp_dir, scm, dvc, exp_stage, caplog):
    dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    with open("dvc.yaml", "a", encoding="utf-8") as fd:
        fd.write("breaking the yaml!")

    results = dvc.experiments.show()
    assert results[0].error
    assert results[0].error.type == "YAMLSyntaxError"

    for exp_range in results[1].experiments:
        assert not any(exp.error for exp in exp_range)


def test_show_csv(tmp_dir, scm, dvc, exp_stage, capsys):
    import time

    baseline_rev = scm.get_rev()

    def _get_rev_isotimestamp(rev):
        return datetime.fromtimestamp(  # noqa: DTZ006
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
        ",master,baseline,{},,1,1,{}".format(  # noqa: UP032
            _get_rev_isotimestamp(baseline_rev), data_hash
        )
        in cap.out
    )
    assert (
        f"{ref_info1.name},{rev1[:7]},branch_base,{_get_rev_isotimestamp(rev1)},,2,2,{data_hash}"
        in cap.out
    )
    assert (
        f"{ref_info2.name},{rev2[:7]},branch_commit,{_get_rev_isotimestamp(rev2)},,3,3,{data_hash}"
        in cap.out
    )


def test_show_only_changed(tmp_dir, dvc, scm, capsys, copy_script):
    params_file = tmp_dir / "params.yaml"
    params_data = {"foo": 1, "goobar": 1}
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


@pytest.mark.vscode
def test_show_outs(tmp_dir, dvc, scm, erepo_dir, copy_script):
    params_file = tmp_dir / "params.yaml"
    params_data = {"foo": 1, "bar": 1}
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

    results = dvc.experiments.show()
    assert results[0].dumpd() == {
        "rev": "workspace",
        "name": None,
        "data": make_data(
            params=ANY,
            outs={
                "out": {
                    "hash": ANY,
                    "size": ANY,
                    "nfiles": None,
                    "use_cache": True,
                    "is_data_source": False,
                }
            },
        ),
        "error": None,
        "experiments": None,
    }

    tmp_dir.dvc_gen("out_add", "foo", commit="dvc add output")
    results = dvc.experiments.show()
    assert results[0].dumpd() == {
        "rev": "workspace",
        "name": None,
        "data": make_data(
            params=ANY,
            outs={
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
            },
        ),
        "error": None,
        "experiments": None,
    }

    with erepo_dir.chdir():
        erepo_dir.dvc_gen("out", "out content", commit="create out")

    dvc.imp(os.fspath(erepo_dir), "out", "out_imported")

    results = dvc.experiments.show()
    assert results[0].dumpd() == {
        "rev": "workspace",
        "name": None,
        "data": make_data(
            params=ANY,
            outs={
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
            },
        ),
        "error": None,
        "experiments": None,
    }


def test_metrics_renaming(tmp_dir, dvc, scm, capsys, copy_script):
    params_file = tmp_dir / "params.yaml"
    params_data = {"foo": 1}
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
    scm.add(["dvc.yaml", "dvc.lock", "params.yaml", "scores.yaml"])
    scm.commit("scores.yaml")
    scores_rev = scm.get_rev()

    capsys.readouterr()
    assert main(["exp", "show", "--csv", "-A"]) == 0
    cap = capsys.readouterr()

    def _get_rev_isotimestamp(rev):
        return datetime.fromtimestamp(  # noqa: DTZ006
            scm.gitpython.repo.rev_parse(rev).committed_date
        ).isoformat()

    assert f",master,baseline,{_get_rev_isotimestamp(scores_rev)},,1,,1" in cap.out
    assert (
        ",{},baseline,{},,,1,1".format(  # noqa: UP032
            metrics_rev[:7], _get_rev_isotimestamp(metrics_rev)
        )
        in cap.out
    )


def test_show_sorted_deps(tmp_dir, dvc, scm, capsys):
    tmp_dir.gen("a", "a")
    tmp_dir.gen("b", "b")
    tmp_dir.gen("c", "c")
    tmp_dir.gen("z", "z")

    dvc.run(cmd="echo foo", name="deps", deps=["a", "b", "z", "c"])

    capsys.readouterr()
    assert main(["exp", "show", "--csv"]) == 0
    cap = capsys.readouterr()
    assert "a,b,c,z" in cap.out


@pytest.mark.vscode
def test_show_queued_error(tmp_dir, scm, dvc, exp_stage, mocker):
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

    results = dvc.experiments.show()[1].experiments
    assert len(results) == 2
    queued = results[0]
    assert queued.executor.state == "queued"
    errored = results[1]
    assert errored.revs[0].error


@pytest.mark.vscode
def test_show_completed_error(tmp_dir, scm, dvc, exp_stage, mocker):
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
    results = dvc.experiments.show()[1].experiments
    assert len(results) == 1
    assert not results[0].revs[0].error


@pytest.mark.vscode
def test_show_baseline_error(tmp_dir, scm, dvc, exp_stage, mocker):
    baseline_rev = scm.get_rev()

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

    results = dvc.experiments.show()
    assert results[1].error
    assert len(results[1].experiments) == 1
