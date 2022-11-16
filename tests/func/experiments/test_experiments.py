import itertools
import logging
import os
import stat

import pytest
from funcy import first

from dvc.dvcfile import PIPELINE_FILE
from dvc.exceptions import ReproductionError
from dvc.repo.experiments.queue.base import BaseStashQueue
from dvc.repo.experiments.utils import exp_refs_by_rev
from dvc.scm import resolve_rev
from dvc.stage.exceptions import StageFileDoesNotExistError
from dvc.utils.serialize import PythonFileCorruptedError
from tests.scripts import COPY_SCRIPT


@pytest.mark.parametrize("name", [None, "foo"])
def test_new_simple(tmp_dir, scm, dvc, exp_stage, mocker, name, workspace):
    baseline = scm.get_rev()
    tmp_dir.gen("params.yaml", "foo: 2")

    new_mock = mocker.spy(dvc.experiments, "new")
    results = dvc.experiments.run(
        exp_stage.addressing, name=name, tmp_dir=not workspace
    )
    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))
    assert ref_info and ref_info.baseline_sha == baseline

    new_mock.assert_called_once()
    fs = scm.get_fs(exp)
    with fs.open("metrics.yaml", mode="r", encoding="utf-8") as fobj:
        assert fobj.read().strip() == "foo: 2"

    if workspace:
        assert (tmp_dir / "metrics.yaml").read_text().strip() == "foo: 2"

    exp_name = name if name else ref_info.name
    assert dvc.experiments.get_exact_name([exp])[exp] == exp_name
    assert resolve_rev(scm, exp_name) == exp


def test_experiment_exists(tmp_dir, scm, dvc, exp_stage, mocker, workspace):
    from dvc.repo.experiments.exceptions import ExperimentExistsError

    dvc.experiments.run(
        exp_stage.addressing,
        name="foo",
        params=["foo=2"],
        tmp_dir=not workspace,
    )

    new_mock = mocker.spy(BaseStashQueue, "_stash_exp")
    with pytest.raises(ExperimentExistsError):
        dvc.experiments.run(
            exp_stage.addressing,
            name="foo",
            params=["foo=3"],
            tmp_dir=not workspace,
        )
    new_mock.assert_not_called()

    results = dvc.experiments.run(
        exp_stage.addressing,
        name="foo",
        params=["foo=3"],
        force=True,
        tmp_dir=not workspace,
    )
    exp = first(results)

    fs = scm.get_fs(exp)
    with fs.open("metrics.yaml", mode="r", encoding="utf-8") as fobj:
        assert fobj.read().strip() == "foo: 3"


@pytest.mark.skipif(os.name == "nt", reason="Not supported for Windows.")
def test_file_permissions(tmp_dir, scm, dvc, exp_stage, mocker):
    mode = 0o755
    os.chmod(tmp_dir / "copy.py", mode)
    scm.add(["copy.py"])
    scm.commit("set exec")

    tmp_dir.gen("params.yaml", "foo: 2")
    dvc.experiments.run(exp_stage.addressing)
    assert stat.S_IMODE(os.stat(tmp_dir / "copy.py").st_mode) == mode


def test_failed_exp_workspace(
    tmp_dir,
    scm,
    dvc,
    failed_exp_stage,
    mocker,
    capsys,
):
    tmp_dir.gen("params.yaml", "foo: 2")
    with pytest.raises(ReproductionError):
        dvc.experiments.run(failed_exp_stage.addressing)
    assert not dvc.fs.exists(
        os.path.join(dvc.experiments.workspace_queue.pid_dir, "workspace")
    )


def test_apply(tmp_dir, scm, dvc, exp_stage):
    from dvc.exceptions import InvalidArgumentError
    from dvc.repo.experiments.exceptions import ApplyConflictError

    results = dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], tmp_dir=True
    )
    exp_a = first(results)

    results = dvc.experiments.run(
        exp_stage.addressing, params=["foo=3"], tmp_dir=True
    )
    exp_b = first(results)

    with pytest.raises(InvalidArgumentError):
        dvc.experiments.apply("foo")

    dvc.experiments.apply(exp_a)
    assert (tmp_dir / "params.yaml").read_text().strip() == "foo: 2"
    assert (tmp_dir / "metrics.yaml").read_text().strip() == "foo: 2"

    with pytest.raises(ApplyConflictError):
        dvc.experiments.apply(exp_b, force=False)
        # failed apply should revert everything to prior state
        assert (tmp_dir / "params.yaml").read_text().strip() == "foo: 2"
        assert (tmp_dir / "metrics.yaml").read_text().strip() == "foo: 2"

    dvc.experiments.apply(exp_b)
    assert (tmp_dir / "params.yaml").read_text().strip() == "foo: 3"
    assert (tmp_dir / "metrics.yaml").read_text().strip() == "foo: 3"


def test_apply_queued(tmp_dir, scm, dvc, exp_stage):
    from dvc.repo.experiments.exceptions import ApplyConflictError

    metrics_original = (tmp_dir / "metrics.yaml").read_text().strip()
    dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], name="exp-a", queue=True
    )
    dvc.experiments.run(
        exp_stage.addressing, params=["foo=3"], name="exp-b", queue=True
    )
    queue_revs = {
        entry.name: entry.stash_rev
        for entry in dvc.experiments.celery_queue.iter_queued()
    }

    dvc.experiments.apply(queue_revs["exp-a"])
    assert (tmp_dir / "params.yaml").read_text().strip() == "foo: 2"
    assert (tmp_dir / "metrics.yaml").read_text().strip() == metrics_original

    with pytest.raises(ApplyConflictError):
        dvc.experiments.apply(queue_revs["exp-b"], force=False)

    dvc.experiments.apply(queue_revs["exp-b"])
    assert (tmp_dir / "params.yaml").read_text().strip() == "foo: 3"
    assert (tmp_dir / "metrics.yaml").read_text().strip() == metrics_original


def test_apply_untracked(tmp_dir, scm, dvc, exp_stage):
    from dvc.repo.experiments.exceptions import ApplyConflictError

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp = first(results)
    tmp_dir.gen("untracked", "untracked")
    tmp_dir.gen("params.yaml", "conflict")

    with pytest.raises(ApplyConflictError):
        dvc.experiments.apply(exp, force=False)

    assert (tmp_dir / "untracked").read_text() == "untracked"
    assert (tmp_dir / "params.yaml").read_text() == "conflict"

    dvc.experiments.apply(exp, force=True)
    assert (tmp_dir / "untracked").read_text() == "untracked"
    assert (tmp_dir / "params.yaml").read_text().strip() == "foo: 2"


def test_get_baseline(tmp_dir, scm, dvc, exp_stage):
    from dvc.repo.experiments.refs import CELERY_STASH

    init_rev = scm.get_rev()
    assert dvc.experiments.get_baseline(init_rev) is None

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp_rev = first(results)
    assert dvc.experiments.get_baseline(exp_rev) == init_rev

    dvc.experiments.run(exp_stage.addressing, params=["foo=3"], queue=True)
    assert dvc.experiments.get_baseline(f"{CELERY_STASH}@{{0}}") == init_rev

    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("promote exp")
    promote_rev = scm.get_rev()
    assert dvc.experiments.get_baseline(promote_rev) is None

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=4"])
    exp_rev = first(results)
    assert dvc.experiments.get_baseline(exp_rev) == promote_rev

    dvc.experiments.run(exp_stage.addressing, params=["foo=5"], queue=True)
    assert dvc.experiments.get_baseline(f"{CELERY_STASH}@{{0}}") == promote_rev
    assert dvc.experiments.get_baseline(f"{CELERY_STASH}@{{1}}") == init_rev


def test_update_py_params(tmp_dir, scm, dvc, test_queue, copy_script):
    tmp_dir.gen("params.py", "INT = 1\n")
    stage = dvc.run(
        cmd="python copy.py params.py metrics.py",
        metrics_no_cache=["metrics.py"],
        params=["params.py:INT"],
        name="copy-file",
    )
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.py", "metrics.py"])
    scm.commit("init")

    results = dvc.experiments.run(
        stage.addressing, params=["params.py:INT=2"], tmp_dir=True
    )
    exp_a = first(results)

    fs = scm.get_fs(exp_a)
    with fs.open("params.py", mode="r", encoding="utf-8") as fobj:
        assert fobj.read().strip() == "INT = 2"
    with fs.open("metrics.py", mode="r", encoding="utf-8") as fobj:
        assert fobj.read().strip() == "INT = 2"

    tmp_dir.gen(
        "params.py",
        "INT = 1\nFLOAT = 0.001\nDICT = {'a': 1}\n\n"
        "class Train:\n    seed = 2020\n\n"
        "class Klass:\n    def __init__(self):\n        self.a = 111\n",
    )
    stage = dvc.run(
        cmd="python copy.py params.py metrics.py",
        metrics_no_cache=["metrics.py"],
        params=["params.py:INT,FLOAT,DICT,Train,Klass"],
        name="copy-file",
    )
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.py", "metrics.py"])
    scm.commit("init")

    results = dvc.experiments.run(
        stage.addressing,
        params=[
            "params.py:FLOAT=0.1",
            "params.py:Train.seed=2121",
            "params.py:Klass.a=222",
        ],
        tmp_dir=True,
    )
    exp_a = first(results)

    result = (
        "INT = 1\nFLOAT = 0.1\nDICT = {'a': 1}\n\n"
        "class Train:\n    seed = 2121\n\n"
        "class Klass:\n    def __init__(self):\n        self.a = 222"
    )

    def _dos2unix(text):
        if os.name != "nt":
            return text

        # NOTE: git on windows will use CRLF, so we have to convert it to LF
        # in order to compare with the original
        return text.replace("\r\n", "\n")

    fs = scm.get_fs(exp_a)
    with fs.open("params.py", mode="r", encoding="utf-8") as fobj:
        assert _dos2unix(fobj.read().strip()) == result
    with fs.open("metrics.py", mode="r", encoding="utf-8") as fobj:
        assert _dos2unix(fobj.read().strip()) == result

    tmp_dir.gen("params.py", "INT = 1\n")
    stage = dvc.run(
        cmd="python copy.py params.py metrics.py",
        metrics_no_cache=["metrics.py"],
        params=["params.py:INT"],
        name="copy-file",
    )
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.py", "metrics.py"])
    scm.commit("init")

    with pytest.raises(PythonFileCorruptedError):
        dvc.experiments.run(
            stage.addressing, params=["params.py:INT=2a"], tmp_dir=True
        )


def test_detached_parent(tmp_dir, scm, dvc, exp_stage, mocker):
    detached_rev = scm.get_rev()

    tmp_dir.gen("params.yaml", "foo: 2")
    dvc.reproduce(exp_stage.addressing)
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("v2")

    scm.checkout(detached_rev)
    assert scm.gitpython.repo.head.is_detached
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])

    exp_rev = first(results)
    assert dvc.experiments.get_baseline(exp_rev) == detached_rev
    assert (tmp_dir / "params.yaml").read_text().strip() == "foo: 3"


def test_branch(tmp_dir, scm, dvc, exp_stage):
    from dvc.exceptions import InvalidArgumentError

    with pytest.raises(InvalidArgumentError):
        dvc.experiments.branch("foo", "branch")

    scm.branch("branch-exists")

    results = dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], name="foo"
    )
    exp_a = first(results)
    ref_a = dvc.experiments.get_branch_by_rev(exp_a)

    with pytest.raises(InvalidArgumentError):
        dvc.experiments.branch("foo", "branch-exists")
    dvc.experiments.branch("foo", "branch-name")
    dvc.experiments.branch(exp_a, "branch-rev")
    dvc.experiments.branch(ref_a, "branch-ref")

    for name in ["branch-name", "branch-rev", "branch-ref"]:
        assert name in scm.list_branches()
        assert scm.resolve_rev(name) == exp_a

    tmp_dir.scm_gen({"new_file": "new_file"}, commit="new baseline")
    results = dvc.experiments.run(
        exp_stage.addressing, params=["foo=2"], name="foo"
    )
    exp_b = first(results)
    ref_b = dvc.experiments.get_branch_by_rev(exp_b)

    with pytest.raises(InvalidArgumentError):
        dvc.experiments.branch("foo", "branch-name")
    dvc.experiments.branch(ref_b, "branch-ref-b")

    assert "branch-ref-b" in scm.list_branches()
    assert scm.resolve_rev("branch-ref-b") == exp_b


def test_no_scm(tmp_dir):
    from dvc.repo import Repo as DvcRepo
    from dvc.scm import NoSCMError

    dvc = DvcRepo.init(no_scm=True)

    for cmd in [
        "apply",
        "branch",
        "diff",
        "show",
        "run",
        "gc",
        "push",
        "pull",
        "ls",
    ]:
        with pytest.raises(NoSCMError):
            getattr(dvc.experiments, cmd)()


def test_untracked(tmp_dir, scm, dvc, caplog, workspace, copy_script):
    tmp_dir.scm_gen("params.yaml", "foo: 1", commit="track params")
    stage = dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        deps=["copy.py"],
        name="copy-file",
        no_exec=True,
    )

    # copy.py is untracked
    # with caplog.at_level(logging.ERROR):
    #     results = dvc.experiments.run(
    #         stage.addressing, params=["foo=2"], tmp_dir=True
    #     )
    #     assert "Failed to reproduce experiment" in caplog.text
    #     assert not results

    # dvc.yaml, copy.py are staged as new file but not committed
    scm.add(["dvc.yaml", "copy.py"])
    results = dvc.experiments.run(
        stage.addressing, params=["foo=2"], tmp_dir=not workspace
    )
    exp = first(results)
    fs = scm.get_fs(exp)
    assert fs.exists("dvc.yaml")
    assert fs.exists("dvc.lock")
    assert fs.exists("copy.py")
    with fs.open("metrics.yaml", mode="r", encoding="utf-8") as fobj:
        assert fobj.read().strip() == "foo: 2"


def test_packed_args_exists(tmp_dir, scm, dvc, exp_stage, caplog):
    from dvc.repo.experiments.executor.base import BaseExecutor

    tmp_dir.scm_gen(
        tmp_dir / ".dvc" / "tmp" / BaseExecutor.PACKED_ARGS_FILE,
        "",
        commit="commit args file",
    )

    with caplog.at_level(logging.WARNING):
        dvc.experiments.run(exp_stage.addressing)
        assert "Temporary DVC file" in caplog.text


def _prepare_experiments(tmp_dir, scm, dvc, exp_stage):
    baseline_a = scm.get_rev()
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp_a = first(results)
    ref_info_a = first(exp_refs_by_rev(scm, exp_a))

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    exp_b = first(results)
    ref_info_b = first(exp_refs_by_rev(scm, exp_b))

    tmp_dir.scm_gen("new", "new", commit="new")
    baseline_c = scm.get_rev()
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=4"])
    exp_c = first(results)
    ref_info_c = first(exp_refs_by_rev(scm, exp_c))

    return baseline_a, baseline_c, ref_info_a, ref_info_b, ref_info_c


def test_list(tmp_dir, scm, dvc, exp_stage):
    (
        baseline_a,
        baseline_c,
        ref_info_a,
        ref_info_b,
        ref_info_c,
    ) = _prepare_experiments(tmp_dir, scm, dvc, exp_stage)

    assert dvc.experiments.ls() == {baseline_c: [ref_info_c.name]}

    exp_list = dvc.experiments.ls(rev=ref_info_a.baseline_sha)
    assert {key: set(val) for key, val in exp_list.items()} == {
        baseline_a: {ref_info_a.name, ref_info_b.name}
    }

    exp_list = dvc.experiments.ls(all_commits=True)
    assert {key: set(val) for key, val in exp_list.items()} == {
        baseline_a: {ref_info_a.name, ref_info_b.name},
        baseline_c: {ref_info_c.name},
    }


def test_list_cli(tmp_dir, scm, dvc, capsys, exp_stage):
    from dvc.cli import main

    baseline_a, _, ref_info_a, ref_info_b, ref_info_c = _prepare_experiments(
        tmp_dir, scm, dvc, exp_stage
    )

    # Make sure that we prioritize the current branch name
    scm.checkout("branch", True)

    capsys.readouterr()
    assert main(["exp", "list", "-A"]) == 0
    cap = capsys.readouterr()
    assert set(cap.out.split()) == set(
        ["branch:", baseline_a[:7] + ":"]
        + [ref_info_a.name, ref_info_b.name, ref_info_c.name]
    )


def test_subdir(tmp_dir, scm, dvc, workspace):
    subdir = tmp_dir / "dir"
    subdir.gen("copy.py", COPY_SCRIPT)
    subdir.gen("params.yaml", "foo: 1")

    with subdir.chdir():
        dvc.run(
            cmd="python copy.py params.yaml metrics.yaml",
            metrics_no_cache=["metrics.yaml"],
            params=["foo"],
            name="copy-file",
            no_exec=True,
        )
        scm.add(
            [subdir / "dvc.yaml", subdir / "copy.py", subdir / "params.yaml"]
        )
        scm.commit("init")

        results = dvc.experiments.run(
            PIPELINE_FILE, params=["foo=2"], tmp_dir=not workspace
        )
        assert results

    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))

    fs = scm.get_fs(exp)
    for fname in ["metrics.yaml", "dvc.lock"]:
        assert fs.exists(f"dir/{fname}")
    with fs.open("dir/metrics.yaml", mode="r", encoding="utf-8") as fobj:
        assert fobj.read().strip() == "foo: 2"

    assert dvc.experiments.get_exact_name([exp])[exp] == ref_info.name
    assert resolve_rev(scm, ref_info.name) == exp


def test_subrepo(tmp_dir, scm, workspace):
    from dvc.testing.tmp_dir import make_subrepo

    subrepo = tmp_dir / "dir" / "repo"
    make_subrepo(subrepo, scm)

    subrepo.gen("copy.py", COPY_SCRIPT)
    subrepo.gen("params.yaml", "foo: 1")

    with subrepo.chdir():
        subrepo.dvc.run(
            cmd="python copy.py params.yaml metrics.yaml",
            metrics_no_cache=["metrics.yaml"],
            params=["foo"],
            name="copy-file",
            no_exec=True,
        )
        scm.add(
            [
                subrepo / "dvc.yaml",
                subrepo / "copy.py",
                subrepo / "params.yaml",
            ]
        )
        scm.commit("init")

        results = subrepo.dvc.experiments.run(
            PIPELINE_FILE, params=["foo=2"], tmp_dir=not workspace
        )
        assert results

    exp = first(results)
    ref_info = first(exp_refs_by_rev(scm, exp))

    fs = scm.get_fs(exp)
    for fname in ["metrics.yaml", "dvc.lock"]:
        assert fs.exists(f"dir/repo/{fname}")
    with fs.open("dir/repo/metrics.yaml", mode="r", encoding="utf-8") as fobj:
        assert fobj.read().strip() == "foo: 2"

    assert subrepo.dvc.experiments.get_exact_name([exp])[exp] == ref_info.name
    assert resolve_rev(scm, ref_info.name) == exp


def test_run_celery(tmp_dir, scm, dvc, exp_stage, mocker):
    """Test running with full (non-pytest-celery) dvc-task queue."""
    dvc.experiments.run(exp_stage.addressing, params=["foo=2"], queue=True)
    dvc.experiments.run(exp_stage.addressing, params=["foo=3"], queue=True)
    assert len(dvc.experiments.stash_revs) == 2

    repro_spy = mocker.spy(dvc.experiments, "reproduce_celery")
    results = dvc.experiments.run(run_all=True)
    assert len(results) == 2
    repro_spy.assert_called_once_with(jobs=1)

    expected = {"foo: 2", "foo: 3"}
    metrics = set()
    for exp in results:
        fs = scm.get_fs(exp)
        with fs.open("metrics.yaml", mode="r", encoding="utf-8") as fobj:
            metrics.add(fobj.read().strip())
    assert expected == metrics


def test_run_metrics(tmp_dir, scm, dvc, exp_stage, mocker):
    from dvc.cli import main

    mocker.patch.object(
        dvc.experiments, "run", return_value={"abc123": "abc123"}
    )
    show_mock = mocker.patch.object(dvc.metrics, "show", return_value={})

    main(["exp", "run", "-m"])
    assert show_mock.called_once()


def test_checkout_targets_deps(tmp_dir, scm, dvc, exp_stage):
    from dvc.utils.fs import remove

    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"}, commit="add files")
    stage = dvc.stage.add(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        name="copy-file",
        deps=["copy.py", "foo"],
        force=True,
    )
    remove("foo")
    remove("bar")

    dvc.experiments.run(stage.addressing, params=["foo=2"])
    assert (tmp_dir / "foo").exists()
    assert (tmp_dir / "foo").read_text() == "foo"
    assert not (tmp_dir / "bar").exists()


@pytest.mark.parametrize("tail", ["", "~1", "^"])
def test_fix_exp_head(tmp_dir, scm, tail):
    from dvc.repo.experiments.refs import EXEC_BASELINE
    from dvc.repo.experiments.utils import fix_exp_head

    head = "HEAD" + tail
    assert head == fix_exp_head(scm, head)

    rev = "1" * 40
    scm.set_ref(EXEC_BASELINE, rev)
    assert EXEC_BASELINE + tail == fix_exp_head(scm, head)
    assert "foo" + tail == fix_exp_head(scm, "foo" + tail)


@pytest.mark.parametrize(
    "params, target",
    itertools.product(("foo: 1", "foo: 2"), (True, False)),
)
def test_modified_data_dep(
    tmp_dir, scm, dvc, workspace, params, target, copy_script
):
    tmp_dir.dvc_gen("data", "data")
    tmp_dir.gen("params.yaml", "foo: 1")
    exp_stage = dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        name="copy-file",
        deps=["copy.py", "data"],
    )
    scm.add(
        [
            "dvc.yaml",
            "dvc.lock",
            "copy.py",
            "params.yaml",
            "metrics.yaml",
            "data.dvc",
            ".gitignore",
        ]
    )
    scm.commit("init")

    tmp_dir.gen("params.yaml", params)
    tmp_dir.gen("data", "modified")

    results = dvc.experiments.run(
        exp_stage.addressing if target else None, tmp_dir=not workspace
    )
    exp = first(results)

    for rev in dvc.brancher(revs=[exp]):
        if rev != exp:
            continue
        with dvc.dvcfs.open("metrics.yaml") as fobj:
            assert fobj.read().strip() == params
        with dvc.dvcfs.open("data") as fobj:
            assert fobj.read().strip() == "modified"

    if workspace:
        assert (tmp_dir / "metrics.yaml").read_text().strip() == params
        assert (tmp_dir / "data").read_text().strip() == "modified"


def test_exp_run_recursive(tmp_dir, scm, dvc, run_copy_metrics):
    tmp_dir.dvc_gen("metric_t.json", '{"foo": 1}')
    run_copy_metrics(
        "metric_t.json", "metric.json", metrics=["metric.json"], no_exec=True
    )
    assert dvc.experiments.run(".", recursive=True)
    assert (tmp_dir / "metric.json").parse() == {"foo": 1}


def test_experiment_name_invalid(tmp_dir, scm, dvc, exp_stage, mocker):
    from dvc.exceptions import InvalidArgumentError

    new_mock = mocker.spy(BaseStashQueue, "_stash_exp")
    with pytest.raises(InvalidArgumentError):
        dvc.experiments.run(
            exp_stage.addressing,
            name="fo^o",
            params=["foo=3"],
        )
    new_mock.assert_not_called()


def test_experiments_workspace_not_log_exception(caplog, dvc, scm):
    """Experiments run in workspace should not log exception.

    Instead it should just leave it to be handled in the main entrypoints.
    """
    with caplog.at_level(logging.ERROR):
        with pytest.raises(StageFileDoesNotExistError):
            dvc.experiments.run()

    assert not caplog.text
