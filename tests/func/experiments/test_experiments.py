import logging
from textwrap import dedent

import pytest
from funcy import first

from dvc.repo.experiments import Experiments
from dvc.utils.serialize import PythonFileCorruptedError
from tests.func.test_repro_multistage import COPY_SCRIPT

CHECKPOINT_SCRIPT_FORMAT = dedent(
    """\
    import os
    import sys
    import shutil
    from time import sleep

    from dvc.api import make_checkpoint

    checkpoint_file = {}
    checkpoint_iterations = int({})
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as fobj:
            try:
                value = int(fobj.read())
            except ValueError:
                value = 0
    else:
        with open(checkpoint_file, "w"):
            pass
        value = 0

    shutil.copyfile({}, {})

    if os.getenv("DVC_CHECKPOINT"):
        for _ in range(checkpoint_iterations):
            value += 1
            with open(checkpoint_file, "w") as fobj:
                fobj.write(str(value))
            make_checkpoint()
"""
)
CHECKPOINT_SCRIPT = CHECKPOINT_SCRIPT_FORMAT.format(
    "sys.argv[1]", "sys.argv[2]", "sys.argv[3]", "sys.argv[4]"
)


@pytest.fixture
def checkpoint_stage(tmp_dir, scm, dvc):
    tmp_dir.gen("checkpoint.py", CHECKPOINT_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")
    stage = dvc.run(
        cmd="python checkpoint.py foo 5 params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        checkpoints=["foo"],
        no_exec=True,
        name="checkpoint-file",
    )
    scm.add(["dvc.yaml", "checkpoint.py", "params.yaml"])
    scm.commit("init")
    return stage


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
    dvc.experiments.run(stage.addressing)

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
    dvc.experiments.run(stage.addressing)
    scm.add(["dvc.yaml", "dvc.lock", "params.yaml", "metrics.yaml"])
    scm.commit("promote experiment")
    expected_revs.append(scm.get_rev())

    tmp_dir.gen("params.yaml", "foo: 3")
    dvc.experiments.run(stage.addressing)

    exp_scm = dvc.experiments.scm
    for rev in expected_revs:
        assert exp_scm.has_rev(rev)


@pytest.mark.parametrize(
    "change, expected",
    [
        ["foo.1.baz=3", "foo: [bar: 1, baz: 3]"],
        ["foo.0=bar", "foo: [bar, baz: 2]"],
        ["foo.1=- baz\n- goo", "foo: [bar: 1, [baz, goo]]"],
    ],
)
def test_modify_list_param(tmp_dir, scm, dvc, mocker, change, expected):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: [bar: 1, baz: 2]")
    stage = dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        name="copy-file",
    )
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("init")

    new_mock = mocker.spy(dvc.experiments, "new")
    dvc.experiments.run(stage.addressing, params=[change])

    new_mock.assert_called_once()
    assert (
        tmp_dir / ".dvc" / "experiments" / "metrics.yaml"
    ).read_text().strip() == expected


def test_checkout(tmp_dir, scm, dvc):
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

    results = dvc.experiments.run(stage.addressing, params=["foo=2"])
    exp_a = first(results)

    results = dvc.experiments.run(stage.addressing, params=["foo=3"])
    exp_b = first(results)

    dvc.experiments.checkout(exp_a)
    assert (tmp_dir / "params.yaml").read_text().strip() == "foo: 2"
    assert (tmp_dir / "metrics.yaml").read_text().strip() == "foo: 2"

    dvc.experiments.checkout(exp_b)
    assert (tmp_dir / "params.yaml").read_text().strip() == "foo: 3"
    assert (tmp_dir / "metrics.yaml").read_text().strip() == "foo: 3"


def test_get_baseline(tmp_dir, scm, dvc):
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
    init_rev = scm.get_rev()
    assert dvc.experiments.get_baseline(init_rev) is None

    results = dvc.experiments.run(stage.addressing, params=["foo=2"])
    exp_rev = first(results)
    assert dvc.experiments.get_baseline(exp_rev) == init_rev

    dvc.experiments.run(stage.addressing, params=["foo=3"], queue=True)
    assert dvc.experiments.get_baseline("stash@{0}") == init_rev

    dvc.experiments.checkout(exp_rev)
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("promote exp")
    promote_rev = scm.get_rev()

    results = dvc.experiments.run(stage.addressing, params=["foo=4"])
    exp_rev = first(results)
    assert dvc.experiments.get_baseline(promote_rev) is None
    assert dvc.experiments.get_baseline(exp_rev) == promote_rev

    dvc.experiments.run(stage.addressing, params=["foo=5"], queue=True)
    assert dvc.experiments.get_baseline("stash@{0}") == promote_rev
    assert dvc.experiments.get_baseline("stash@{1}") == init_rev


def test_update_py_params(tmp_dir, scm, dvc):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("params.py", "INT = 1\n")
    stage = dvc.run(
        cmd="python copy.py params.py metrics.py",
        metrics_no_cache=["metrics.py"],
        params=["params.py:INT"],
        name="copy-file",
    )
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.py", "metrics.py"])
    scm.commit("init")

    results = dvc.experiments.run(stage.addressing, params=["params.py:INT=2"])
    exp_a = first(results)

    dvc.experiments.checkout(exp_a)
    assert (tmp_dir / "params.py").read_text().strip() == "INT = 2"
    assert (tmp_dir / "metrics.py").read_text().strip() == "INT = 2"

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
        params=["params.py:FLOAT=0.1,Train.seed=2121,Klass.a=222"],
    )
    exp_a = first(results)

    result = (
        "INT = 1\nFLOAT = 0.1\nDICT = {'a': 1}\n\n"
        "class Train:\n    seed = 2121\n\n"
        "class Klass:\n    def __init__(self):\n        self.a = 222"
    )

    dvc.experiments.checkout(exp_a)
    assert (tmp_dir / "params.py").read_text().strip() == result
    assert (tmp_dir / "metrics.py").read_text().strip() == result

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
        dvc.experiments.run(stage.addressing, params=["params.py:INT=2a"])


def test_extend_branch(tmp_dir, scm, dvc):
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

    results = dvc.experiments.run(stage.addressing, params=["foo=2"])
    exp_a = first(results)
    exp_branch = dvc.experiments._get_branch_containing(exp_a)

    results = dvc.experiments.run(
        stage.addressing,
        params=["foo=3"],
        branch=exp_branch,
        apply_workspace=False,
    )
    exp_b = first(results)

    assert exp_a != exp_b
    assert dvc.experiments._get_branch_containing(exp_b) == exp_branch

    dvc.experiments.checkout(exp_a)
    assert (tmp_dir / "params.yaml").read_text().strip() == "foo: 2"
    assert (tmp_dir / "metrics.yaml").read_text().strip() == "foo: 2"

    dvc.experiments.checkout(exp_b)
    assert (tmp_dir / "params.yaml").read_text().strip() == "foo: 3"
    assert (tmp_dir / "metrics.yaml").read_text().strip() == "foo: 3"


def test_detached_parent(tmp_dir, scm, dvc, mocker):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")
    stage = dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        name="copy-file",
    )
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("v1")
    detached_rev = scm.get_rev()

    tmp_dir.gen("params.yaml", "foo: 2")
    dvc.reproduce(stage.addressing)
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("v2")

    scm.checkout(detached_rev)
    assert scm.repo.head.is_detached
    results = dvc.experiments.run(stage.addressing, params=["foo=3"])

    exp_rev = first(results)
    assert dvc.experiments.get_baseline(exp_rev) == detached_rev
    assert (tmp_dir / "params.yaml").read_text().strip() == "foo: 3"
    assert (tmp_dir / "metrics.yaml").read_text().strip() == "foo: 3"


def test_new_checkpoint(tmp_dir, scm, dvc, checkpoint_stage, mocker):
    new_mock = mocker.spy(dvc.experiments, "new")
    dvc.experiments.run(checkpoint_stage.addressing, params=["foo=2"])

    new_mock.assert_called_once()
    assert (tmp_dir / "foo").read_text() == "5"
    assert (
        tmp_dir / ".dvc" / "experiments" / "metrics.yaml"
    ).read_text().strip() == "foo: 2"


@pytest.mark.parametrize("last", [True, False])
def test_resume_checkpoint(tmp_dir, scm, dvc, checkpoint_stage, mocker, last):
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"]
    )
    if last:
        exp_rev = Experiments.LAST_CHECKPOINT
    else:
        exp_rev = first(results)

    dvc.experiments.run(
        checkpoint_stage.addressing, checkpoint_continue=exp_rev
    )

    assert (tmp_dir / "foo").read_text() == "10"
    assert (
        tmp_dir / ".dvc" / "experiments" / "metrics.yaml"
    ).read_text().strip() == "foo: 2"


def test_reset_checkpoint(tmp_dir, scm, dvc, checkpoint_stage, mocker, caplog):
    dvc.experiments.run(checkpoint_stage.addressing)
    scm.repo.git.reset(hard=True)
    scm.repo.git.clean(force=True)

    with caplog.at_level(logging.ERROR):
        results = dvc.experiments.run(checkpoint_stage.addressing)
        assert len(results) == 0
        assert "already exists" in caplog.text

    dvc.experiments.run(checkpoint_stage.addressing, force=True)

    assert (tmp_dir / "foo").read_text() == "5"
    assert (
        tmp_dir / ".dvc" / "experiments" / "metrics.yaml"
    ).read_text().strip() == "foo: 1"
