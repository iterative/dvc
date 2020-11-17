import logging
import os
import stat

import pytest
from funcy import first

from dvc.utils.serialize import PythonFileCorruptedError
from tests.func.test_repro_multistage import COPY_SCRIPT


def test_new_simple(tmp_dir, scm, dvc, exp_stage, mocker):
    tmp_dir.gen("params.yaml", "foo: 2")

    new_mock = mocker.spy(dvc.experiments, "new")
    dvc.experiments.run(exp_stage.addressing)

    new_mock.assert_called_once()
    assert (
        tmp_dir / ".dvc" / "experiments" / "metrics.yaml"
    ).read_text() == "foo: 2"


@pytest.mark.skipif(os.name == "nt", reason="Not supported for Windows.")
def test_file_permissions(tmp_dir, scm, dvc, exp_stage, mocker):
    mode = 0o755
    os.chmod(tmp_dir / "copy.py", mode)
    scm.add(["copy.py"])
    scm.commit("set exec")

    tmp_dir.gen("params.yaml", "foo: 2")
    dvc.experiments.run(exp_stage.addressing)
    assert stat.S_IMODE(os.stat(tmp_dir / "copy.py").st_mode) == mode


def test_failed_exp(tmp_dir, scm, dvc, exp_stage, mocker, caplog):
    from dvc.stage.exceptions import StageCmdFailedError

    tmp_dir.gen("params.yaml", "foo: 2")

    mocker.patch(
        "dvc.stage.run.cmd_run",
        side_effect=StageCmdFailedError(exp_stage.cmd, -1),
    )
    with caplog.at_level(logging.ERROR):
        dvc.experiments.run(exp_stage.addressing)
        assert "Failed to reproduce experiment" in caplog.text


def test_update_with_pull(tmp_dir, scm, dvc, exp_stage, mocker):
    expected_revs = [scm.get_rev()]

    tmp_dir.gen("params.yaml", "foo: 2")
    dvc.experiments.run(exp_stage.addressing)
    scm.add(["dvc.yaml", "dvc.lock", "params.yaml", "metrics.yaml"])
    scm.commit("promote experiment")
    expected_revs.append(scm.get_rev())

    tmp_dir.gen("params.yaml", "foo: 3")
    dvc.experiments.run(exp_stage.addressing)

    exp_scm = dvc.experiments.scm
    for rev in expected_revs:
        assert exp_scm.has_rev(rev)


@pytest.mark.parametrize(
    "changes, expected",
    [
        [["foo=baz"], "{foo: baz, goo: {bag: 3}, lorem: false}"],
        [["foo=baz,goo=bar"], "{foo: baz, goo: bar, lorem: false}"],
        [
            ["goo.bag=4"],
            "{foo: [bar: 1, baz: 2], goo: {bag: 4}, lorem: false}",
        ],
        [["foo[0]=bar"], "{foo: [bar, baz: 2], goo: {bag: 3}, lorem: false}"],
        [
            ["foo[1].baz=3"],
            "{foo: [bar: 1, baz: 3], goo: {bag: 3}, lorem: false}",
        ],
        [
            ["foo[1]=- baz\n- goo"],
            "{foo: [bar: 1, [baz, goo]], goo: {bag: 3}, lorem: false}",
        ],
        [
            ["lorem.ipsum=3"],
            "{foo: [bar: 1, baz: 2], goo: {bag: 3}, lorem: {ipsum: 3}}",
        ],
    ],
)
def test_modify_params(tmp_dir, scm, dvc, mocker, changes, expected):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen(
        "params.yaml", "{foo: [bar: 1, baz: 2], goo: {bag: 3}, lorem: false}"
    )
    stage = dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo", "goo", "lorem"],
        name="copy-file",
    )
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("init")

    new_mock = mocker.spy(dvc.experiments, "new")
    dvc.experiments.run(stage.addressing, params=changes)

    new_mock.assert_called_once()
    assert (
        tmp_dir / ".dvc" / "experiments" / "metrics.yaml"
    ).read_text().strip() == expected


def test_checkout(tmp_dir, scm, dvc, exp_stage):
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp_a = first(results)

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])
    exp_b = first(results)

    dvc.experiments.checkout(exp_a)
    assert (tmp_dir / "params.yaml").read_text().strip() == "foo: 2"
    assert (tmp_dir / "metrics.yaml").read_text().strip() == "foo: 2"

    dvc.experiments.checkout(exp_b)
    assert (tmp_dir / "params.yaml").read_text().strip() == "foo: 3"
    assert (tmp_dir / "metrics.yaml").read_text().strip() == "foo: 3"


def test_get_baseline(tmp_dir, scm, dvc, exp_stage):
    init_rev = scm.get_rev()
    assert dvc.experiments.get_baseline(init_rev) is None

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp_rev = first(results)
    assert dvc.experiments.get_baseline(exp_rev) == init_rev

    dvc.experiments.run(exp_stage.addressing, params=["foo=3"], queue=True)
    assert dvc.experiments.get_baseline("stash@{0}") == init_rev

    dvc.experiments.checkout(exp_rev)
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("promote exp")
    promote_rev = scm.get_rev()

    results = dvc.experiments.run(exp_stage.addressing, params=["foo=4"])
    exp_rev = first(results)
    assert dvc.experiments.get_baseline(promote_rev) is None
    assert dvc.experiments.get_baseline(exp_rev) == promote_rev

    dvc.experiments.run(exp_stage.addressing, params=["foo=5"], queue=True)
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


def test_extend_branch(tmp_dir, scm, dvc, exp_stage):
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp_a = first(results)
    exp_branch = dvc.experiments._get_branch_containing(exp_a)

    results = dvc.experiments.run(
        exp_stage.addressing,
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


def test_detached_parent(tmp_dir, scm, dvc, exp_stage, mocker):
    detached_rev = scm.get_rev()

    tmp_dir.gen("params.yaml", "foo: 2")
    dvc.reproduce(exp_stage.addressing)
    scm.add(["dvc.yaml", "dvc.lock", "copy.py", "params.yaml", "metrics.yaml"])
    scm.commit("v2")

    scm.checkout(detached_rev)
    assert scm.repo.head.is_detached
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=3"])

    exp_rev = first(results)
    assert dvc.experiments.get_baseline(exp_rev) == detached_rev
    assert (tmp_dir / "params.yaml").read_text().strip() == "foo: 3"
    assert (tmp_dir / "metrics.yaml").read_text().strip() == "foo: 3"
