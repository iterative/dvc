import itertools
import logging
import os
import stat
from textwrap import dedent

import pytest
from configobj import ConfigObj
from funcy import first

from dvc.dvcfile import PROJECT_FILE
from dvc.env import (
    DVC_EXP_BASELINE_REV,
    DVC_EXP_NAME,
    DVC_STUDIO_OFFLINE,
    DVC_STUDIO_REPO_URL,
    DVC_STUDIO_TOKEN,
    DVC_STUDIO_URL,
)
from dvc.exceptions import DvcException, ReproductionError
from dvc.repo.experiments.exceptions import ExperimentExistsError
from dvc.repo.experiments.queue.base import BaseStashQueue
from dvc.repo.experiments.refs import CELERY_STASH
from dvc.repo.experiments.utils import exp_refs_by_rev
from dvc.scm import SCMError, resolve_rev
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
    assert ref_info
    assert ref_info.baseline_sha == baseline

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


def test_get_baseline(tmp_dir, scm, dvc, exp_stage):
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


def test_update_py_params(tmp_dir, scm, dvc, session_queue, copy_script):
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
        (
            "INT = 1\nFLOAT = 0.001\nDICT = {'a': 1}\n\n"
            "class Train:\n    seed = 2020\n\n"
            "class Klass:\n    def __init__(self):\n        self.a = 111\n"
        ),
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
        dvc.experiments.run(stage.addressing, params=["params.py:INT=2a"], tmp_dir=True)


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

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"], name="foo")
    exp_a = first(results)
    ref_a = dvc.experiments.get_branch_by_rev(exp_a)

    with pytest.raises(InvalidArgumentError):
        dvc.experiments.branch("foo", "branch-exists")
    dvc.experiments.branch("foo")
    dvc.experiments.branch("foo", "branch-name")
    dvc.experiments.branch(exp_a, "branch-rev")
    dvc.experiments.branch(ref_a, "branch-ref")

    for name in ["foo-branch", "branch-name", "branch-rev", "branch-ref"]:
        assert name in scm.list_branches()
        assert scm.resolve_rev(name) == exp_a

    tmp_dir.scm_gen({"new_file": "new_file"}, commit="new baseline")
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"], name="foo")
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
        force=True,
    )

    with caplog.at_level(logging.WARNING):
        dvc.experiments.run(exp_stage.addressing)
        assert "Temporary DVC file" in caplog.text
    assert not (tmp_dir / ".dvc" / "tmp" / BaseExecutor.PACKED_ARGS_FILE).exists()


def test_list(tmp_dir, scm, dvc, exp_stage):
    baseline_a = scm.get_rev()
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp_a = first(results)
    ref_info_a = first(exp_refs_by_rev(scm, exp_a))

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    exp_b = first(results)
    ref_info_b = first(exp_refs_by_rev(scm, exp_b))

    tmp_dir.scm_gen("new", "new", commit="new")
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=4"])
    exp_c = first(results)
    ref_info_c = first(exp_refs_by_rev(scm, exp_c))

    assert dvc.experiments.ls() == {"refs/heads/master": [(ref_info_c.name, exp_c)]}

    exp_list = dvc.experiments.ls(rev=ref_info_a.baseline_sha)
    assert {key: set(val) for key, val in exp_list.items()} == {
        baseline_a: {(ref_info_a.name, exp_a), (ref_info_b.name, exp_b)}
    }

    exp_list = dvc.experiments.ls(rev=[baseline_a, scm.get_rev()])
    assert {key: set(val) for key, val in exp_list.items()} == {
        baseline_a: {(ref_info_a.name, exp_a), (ref_info_b.name, exp_b)},
        "refs/heads/master": {(ref_info_c.name, exp_c)},
    }

    exp_list = dvc.experiments.ls(all_commits=True)
    assert {key: set(val) for key, val in exp_list.items()} == {
        baseline_a: {(ref_info_a.name, exp_a), (ref_info_b.name, exp_b)},
        "refs/heads/master": {(ref_info_c.name, exp_c)},
    }

    scm.checkout("branch", True)
    exp_list = dvc.experiments.ls(all_commits=True)
    assert {key: set(val) for key, val in exp_list.items()} == {
        baseline_a: {(ref_info_a.name, exp_a), (ref_info_b.name, exp_b)},
        "refs/heads/branch": {(ref_info_c.name, exp_c)},
    }


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
        scm.add([subdir / "dvc.yaml", subdir / "copy.py", subdir / "params.yaml"])
        scm.commit("init")

        results = dvc.experiments.run(
            PROJECT_FILE, params=["foo=2"], tmp_dir=not workspace
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


def test_subrepo(tmp_dir, request, scm, workspace):
    from dvc.testing.tmp_dir import make_subrepo

    subrepo = tmp_dir / "dir" / "repo"
    make_subrepo(subrepo, scm)
    request.addfinalizer(subrepo.dvc.close)

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
            PROJECT_FILE, params=["foo=2"], tmp_dir=not workspace
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
def test_modified_data_dep(tmp_dir, scm, dvc, workspace, params, target, copy_script):
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
        "metric_t.json",
        "metric.json",
        metrics=["metric.json"],
        no_exec=True,
        name="copy-metric",
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


@pytest.mark.vscode
def test_run_env(tmp_dir, dvc, scm, mocker):
    dump_run_env = dedent(
        """\
        import os
        from dvc.env import (
            DVC_EXP_BASELINE_REV,
            DVC_EXP_NAME,
            DVC_STUDIO_OFFLINE,
            DVC_STUDIO_REPO_URL,
            DVC_STUDIO_TOKEN,
            DVC_STUDIO_URL
        )
        for v in (
            DVC_EXP_BASELINE_REV,
            DVC_EXP_NAME,
            DVC_STUDIO_OFFLINE,
            DVC_STUDIO_REPO_URL,
            DVC_STUDIO_TOKEN,
            DVC_STUDIO_URL
        ):
            with open(v, "w") as f:
                f.write(os.environ.get(v, ""))
        """
    )
    mocker.patch(
        "dvc.repo.experiments.queue.base.get_studio_config",
        return_value={
            "token": "TOKEN",
            "repo_url": "REPO_URL",
            "url": "BASE_URL",
            "offline": "false",
        },
    )
    (tmp_dir / "dump_run_env.py").write_text(dump_run_env)
    baseline = scm.get_rev()
    dvc.stage.add(
        cmd="python dump_run_env.py",
        name="run_env",
    )
    dvc.experiments.run()
    assert (tmp_dir / DVC_EXP_BASELINE_REV).read_text().strip() == baseline
    assert (tmp_dir / DVC_EXP_NAME).read_text().strip()
    assert (tmp_dir / DVC_STUDIO_TOKEN).read_text().strip() == "TOKEN"
    assert (tmp_dir / DVC_STUDIO_REPO_URL).read_text().strip() == "REPO_URL"
    assert (tmp_dir / DVC_STUDIO_URL).read_text().strip() == "BASE_URL"
    assert (tmp_dir / DVC_STUDIO_OFFLINE).read_text().strip() == "false"

    dvc.experiments.run(name="foo")
    assert (tmp_dir / DVC_EXP_BASELINE_REV).read_text().strip() == baseline
    assert (tmp_dir / DVC_EXP_NAME).read_text().strip() == "foo"


def test_experiment_unchanged(tmp_dir, scm, dvc, exp_stage):
    dvc.experiments.run(exp_stage.addressing)
    dvc.experiments.run(exp_stage.addressing)

    assert len(dvc.experiments.ls()["refs/heads/master"]) == 2


def test_experiment_run_dry(tmp_dir, scm, dvc, exp_stage):
    dvc.experiments.run(exp_stage.addressing, dry=True)

    assert len(dvc.experiments.ls()["master"]) == 0


def test_clean(tmp_dir, scm, dvc, mocker):
    clean = mocker.spy(dvc.experiments.celery_queue.celery, "clean")
    dvc.experiments.clean()
    clean.assert_called_once_with()


def test_experiment_no_commit(tmp_dir):
    from scmrepo.git import Git

    from dvc.repo import Repo

    Git.init(tmp_dir.fs_path).close()

    repo = Repo.init()
    assert repo.scm.no_commits

    try:
        with pytest.raises(SCMError):  # noqa: PT011
            repo.experiments.ls()
    finally:
        repo.close()


def test_local_config_is_propagated_to_tmp(tmp_dir, scm, dvc):
    with dvc.config.edit("local") as conf:
        conf["cache"]["type"] = "hardlink"

    stage = dvc.stage.add(
        cmd="cat .dvc/config.local > file", name="foo", outs_no_cache=["file"]
    )
    scm.add_commit(["dvc.yaml"], message="add dvc.yaml")

    results = dvc.experiments.run(stage.addressing, tmp_dir=True)
    exp = first(results)
    fs = scm.get_fs(exp)

    with fs.open("file") as fobj:
        conf_obj = ConfigObj(fobj)
        assert conf_obj["cache"]["type"] == "hardlink"


@pytest.mark.parametrize("tmp", [True, False])
def test_untracked_top_level_files_are_included_in_exp(tmp_dir, scm, dvc, tmp):
    (tmp_dir / "dvc.yaml").dump(
        {
            "metrics": ["metrics.json"],
            "params": ["params.yaml"],
            "plots": ["plots.csv"],
        }
    )
    stage = dvc.stage.add(
        cmd="touch metrics.json && touch params.yaml && touch plots.csv",
        name="top-level",
    )
    scm.add_commit(["dvc.yaml"], message="add dvc.yaml")
    results = dvc.experiments.run(stage.addressing, tmp_dir=tmp)
    exp = first(results)
    fs = scm.get_fs(exp)
    for file in ["metrics.json", "params.yaml", "plots.csv"]:
        assert fs.exists(file)


@pytest.mark.parametrize("tmp", [True, False])
def test_copy_paths(tmp_dir, scm, dvc, tmp):
    stage = dvc.stage.add(
        cmd="cat file && ls dir",
        name="foo",
    )
    scm.add_commit(["dvc.yaml"], message="add dvc.yaml")

    (tmp_dir / "dir").mkdir()
    (tmp_dir / "dir" / "file").write_text("dir/file")
    scm.ignore(tmp_dir / "dir")
    (tmp_dir / "file").write_text("file")
    scm.ignore(tmp_dir / "file")

    results = dvc.experiments.run(
        stage.addressing, tmp_dir=tmp, copy_paths=["dir", "file"]
    )
    exp = first(results)
    fs = scm.get_fs(exp)
    assert not fs.exists("dir")
    assert not fs.exists("file")


def test_copy_paths_errors(tmp_dir, scm, dvc, mocker):
    stage = dvc.stage.add(
        cmd="echo foo",
        name="foo",
    )
    scm.add_commit(["dvc.yaml"], message="add dvc.yaml")

    with pytest.raises(DvcException, match="Unable to copy"):
        dvc.experiments.run(stage.addressing, tmp_dir=True, copy_paths=["foo"])

    (tmp_dir / "foo").write_text("foo")
    mocker.patch("shutil.copy", side_effect=OSError)

    with pytest.raises(DvcException, match="Unable to copy"):
        dvc.experiments.run(stage.addressing, tmp_dir=True, copy_paths=["foo"])


def test_mixed_git_dvc_out(tmp_dir, scm, dvc, exp_stage):
    (tmp_dir / "dir").mkdir()
    dir_metrics = os.path.join("dir", "metrics.yaml")
    dvc.stage.add(
        cmd=f"python copy.py params.yaml {dir_metrics}",
        metrics=[dir_metrics],
        params=["foo"],
        name="copy-file",
        deps=["copy.py"],
        force=True,
    )
    dvc.stage.add(
        cmd=f"python copy.py {dir_metrics} metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        name="copy-dir-file",
        deps=["dir"],
    )
    scm.add(["dvc.yaml", "dvc.lock"])
    scm.commit("add dir stage")

    exp = first(dvc.experiments.run())
    assert (tmp_dir / "dir" / "metrics.yaml").exists()
    git_fs = scm.get_fs(exp)
    assert not git_fs.exists("dir/metrics.yaml")


@pytest.mark.parametrize("tmp", [True, False])
def test_custom_commit_message(tmp_dir, scm, dvc, tmp):
    stage = dvc.stage.add(
        cmd="echo foo",
        name="foo",
    )
    scm.add_commit(["dvc.yaml"], message="add dvc.yaml")

    exp = first(
        dvc.experiments.run(
            stage.addressing, tmp_dir=tmp, message="custom commit message"
        )
    )
    assert scm.gitpython.repo.commit(exp).message == "custom commit message"
