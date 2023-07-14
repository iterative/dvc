import json
import os
from textwrap import dedent
from typing import Dict, List

import pytest

from dvc import api

TRAIN_METRICS: List[Dict[str, Dict[str, float]]] = [
    {
        "avg_prec": {"train": 0.85, "val": 0.75},
        "roc_auc": {"train": 0.80, "val": 0.70},
    },
    {
        "avg_prec": {"train": 0.97, "val": 0.92},
        "roc_auc": {"train": 0.98, "val": 0.94},
    },
]
TEST_METRICS: List[Dict[str, Dict[str, float]]] = [
    {"avg_prec": {"test": 0.72}, "roc_auc": {"test": 0.77}},
    {
        "avg_prec": {"test": 0.91},
        "roc_auc": {"test": 0.92},
    },
]


@pytest.fixture
def params_repo(tmp_dir, scm, dvc):
    tmp_dir.gen("params.yaml", "foo: 1")
    tmp_dir.gen("params.json", '{"bar": 2, "foobar": 3}')
    tmp_dir.gen("other_params.json", '{"foo": {"bar": 4}}')

    dvc.run(
        name="stage-0",
        cmd="echo stage-0",
    )

    dvc.run(
        name="stage-1",
        cmd="echo stage-1",
        params=["foo", "params.json:bar"],
    )

    dvc.run(
        name="stage-2",
        cmd="echo stage-2",
        params=["other_params.json:foo"],
    )

    dvc.run(
        name="stage-3",
        cmd="echo stage-2",
        params=["params.json:foobar"],
    )

    scm.add(
        [
            "params.yaml",
            "params.json",
            "other_params.json",
            "dvc.yaml",
            "dvc.lock",
        ]
    )
    scm.commit("commit dvc files")

    tmp_dir.gen("params.yaml", "foo: 5")
    scm.add(["params.yaml"])
    scm.commit("update params.yaml")


@pytest.fixture
def metrics_repo(tmp_dir, scm, dvc, run_copy_metrics):
    dvc.run(name="prepare", cmd="echo preparing data")
    scm.add(["dvc.yaml", "dvc.lock"])
    scm.commit("prepare data")
    sub_dir = tmp_dir / "eval"
    sub_dir.mkdir()
    tmp_dir.gen(
        "tmp_train_val_metrics.json",
        json.dumps(TRAIN_METRICS[0]),
    )
    train_metrics_file = os.path.join(sub_dir, "train_val_metrics.json")
    run_copy_metrics(
        "tmp_train_val_metrics.json",
        train_metrics_file,
        name="train",
        metrics_no_cache=[train_metrics_file],
    )
    (tmp_dir / "tmp_train_val_metrics.json").unlink()

    scm.add(["dvc.yaml", "dvc.lock", train_metrics_file])
    scm.commit("train model")

    test_metrics_file = os.path.join(sub_dir, "test_metrics.json")
    tmp_dir.gen("tmp_test_metrics.json", json.dumps(TEST_METRICS[0]))
    run_copy_metrics(
        "tmp_test_metrics.json",
        test_metrics_file,
        name="test",
        metrics_no_cache=[test_metrics_file],
    )
    (tmp_dir / "tmp_test_metrics.json").unlink()

    scm.add(["dvc.yaml", "dvc.lock", test_metrics_file])
    scm.commit("test model")

    with tmp_dir.branch("better-model", new=True):
        tmp_dir.gen(
            "tmp_train_val_metrics.json",
            json.dumps(TRAIN_METRICS[1]),
        )
        run_copy_metrics(
            "tmp_train_val_metrics.json",
            train_metrics_file,
            name="train",
            metrics_no_cache=[train_metrics_file],
        )
        (tmp_dir / "tmp_train_val_metrics.json").unlink()

        scm.add(["dvc.yaml", "dvc.lock", train_metrics_file])
        scm.commit("train better model")

        tmp_dir.gen("tmp_test_metrics.json", json.dumps(TEST_METRICS[1]))
        run_copy_metrics(
            "tmp_test_metrics.json",
            test_metrics_file,
            name="test",
            metrics_no_cache=[test_metrics_file],
        )
        (tmp_dir / "tmp_test_metrics.json").unlink()

        scm.add(["dvc.yaml", "dvc.lock", test_metrics_file])
        scm.commit("test better model")

    scm.checkout("master")

    return (
        os.path.relpath(train_metrics_file, tmp_dir),
        os.path.relpath(test_metrics_file, tmp_dir),
    )


def test_params_show_no_args(params_repo):
    assert api.params_show() == {
        "params.yaml:foo": 5,
        "bar": 2,
        "foobar": 3,
        "other_params.json:foo": {"bar": 4},
    }


def test_params_show_targets(params_repo):
    assert api.params_show("params.yaml") == {"foo": 5}
    assert api.params_show("params.yaml", "params.json") == {
        "foo": 5,
        "bar": 2,
        "foobar": 3,
    }
    assert api.params_show("params.yaml", stages="stage-1") == {
        "foo": 5,
    }


def test_params_show_deps(params_repo):
    params = api.params_show(deps=True)
    assert params == {
        "params.yaml:foo": 5,
        "bar": 2,
        "foobar": 3,
        "other_params.json:foo": {"bar": 4},
    }


def test_params_show_stages(params_repo):
    assert api.params_show(stages="stage-2") == {"foo": {"bar": 4}}

    assert api.params_show() == api.params_show(
        stages=["stage-1", "stage-2", "stage-3"]
    )

    assert api.params_show("params.json", stages="stage-3") == {"foobar": 3}

    assert api.params_show(stages="stage-0") == {}


def test_params_show_stage_addressing(tmp_dir, dvc):
    for subdir in ("subdir1", "subdir2"):
        subdir = tmp_dir / subdir
        subdir.mkdir()
        with subdir.chdir():
            subdir.gen("params.yaml", "foo: 1")

            dvc.run(name="stage-0", cmd="echo stage-0", params=["foo"])

    for s in ("subdir1", "subdir2"):
        dvcyaml = os.path.join(s, "dvc.yaml")
        assert api.params_show(stages=f"{dvcyaml}:stage-0") == {"foo": 1}

    with subdir.chdir():
        nested = subdir / "nested"
        nested.mkdir()
        with nested.chdir():
            dvcyaml = os.path.join("..", "dvc.yaml")
            assert api.params_show(stages=f"{dvcyaml}:stage-0") == {"foo": 1}


def test_params_show_revs(params_repo):
    assert api.params_show(rev="HEAD~1") == {
        "params.yaml:foo": 1,
        "bar": 2,
        "foobar": 3,
        "other_params.json:foo": {"bar": 4},
    }


def test_params_show_while_running_stage(tmp_dir, dvc):
    (tmp_dir / "params.yaml").dump({"foo": {"bar": 1}})
    (tmp_dir / "params.json").dump({"bar": 2})

    tmp_dir.gen(
        "merge.py",
        dedent(
            """
            import json
            from dvc import api
            with open("merged.json", "w") as f:
                json.dump(api.params_show(stages="merge"), f)
        """
        ),
    )
    dvc.stage.add(
        name="merge",
        cmd="python merge.py",
        params=["foo.bar", {"params.json": ["bar"]}],
        outs=["merged.json"],
    )

    dvc.reproduce()

    assert (tmp_dir / "merged.json").parse() == {"foo": {"bar": 1}, "bar": 2}


def test_params_show_repo(tmp_dir, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm_gen("params.yaml", "foo: 1", commit="Create params.yaml")
        erepo_dir.dvc.run(
            name="stage-1",
            cmd="echo stage-1",
            params=["foo"],
        )
    assert api.params_show(repo=erepo_dir) == {"foo": 1}


def test_params_show_no_params_found(tmp_dir, dvc):
    # Empty repo
    assert api.params_show() == {}

    # params.yaml but no dvc.yaml
    (tmp_dir / "params.yaml").dump({"foo": 1})
    assert api.params_show() == {"foo": 1}

    # dvc.yaml but no params.yaml
    (tmp_dir / "params.yaml").unlink()
    dvc.stage.add(name="echo", cmd="echo foo")
    assert api.params_show() == {}


def test_params_show_stage_without_params(tmp_dir, dvc):
    tmp_dir.gen("params.yaml", "foo: 1")

    dvc.run(
        name="stage-0",
        cmd="echo stage-0",
    )

    assert api.params_show(stages="stage-0") == {}

    assert api.params_show(deps=True) == {}


def test_params_show_untracked_target(params_repo, tmp_dir):
    tmp_dir.gen("params_foo.yaml", "foo: 1")

    assert api.params_show("params_foo.yaml") == {"foo": 1}

    assert api.params_show("params_foo.yaml", stages="stage-0") == {}

    assert api.params_show("params_foo.yaml", deps=True) == {}


def test_metrics_show_no_args(metrics_repo):
    train_metrics_file, test_metrics_file = metrics_repo
    assert api.metrics_show() == {
        f"{train_metrics_file}:avg_prec": TRAIN_METRICS[0]["avg_prec"],
        f"{train_metrics_file}:roc_auc": TRAIN_METRICS[0]["roc_auc"],
        f"{test_metrics_file}:avg_prec": TEST_METRICS[0]["avg_prec"],
        f"{test_metrics_file}:roc_auc": TEST_METRICS[0]["roc_auc"],
    }


def test_metrics_show_targets(metrics_repo):
    train_metrics_file, test_metrics_file = metrics_repo
    assert api.metrics_show(train_metrics_file) == TRAIN_METRICS[0]
    assert api.metrics_show(test_metrics_file) == TEST_METRICS[0]
    assert api.metrics_show(train_metrics_file, test_metrics_file) == {
        f"{train_metrics_file}:avg_prec": TRAIN_METRICS[0]["avg_prec"],
        f"{train_metrics_file}:roc_auc": TRAIN_METRICS[0]["roc_auc"],
        f"{test_metrics_file}:avg_prec": TEST_METRICS[0]["avg_prec"],
        f"{test_metrics_file}:roc_auc": TEST_METRICS[0]["roc_auc"],
    }


def test_metrics_show_no_metrics_found(tmp_dir, dvc):
    # Empty repo
    assert api.metrics_show() == {}

    # dvc.yaml but no metrics
    dvc.stage.add(name="echo", cmd="echo foo")
    assert api.metrics_show() == {}


def test_metrics_show_rev_without_metrics(metrics_repo):
    assert api.metrics_show(rev="HEAD~2") == {}


def test_metrics_show_rev_with_metrics(metrics_repo):
    train_metrics_file, test_metrics_file = metrics_repo
    assert api.metrics_show(rev="HEAD~1") == TRAIN_METRICS[0]
    assert api.metrics_show(rev="HEAD") == {
        f"{train_metrics_file}:avg_prec": TRAIN_METRICS[0]["avg_prec"],
        f"{train_metrics_file}:roc_auc": TRAIN_METRICS[0]["roc_auc"],
        f"{test_metrics_file}:avg_prec": TEST_METRICS[0]["avg_prec"],
        f"{test_metrics_file}:roc_auc": TEST_METRICS[0]["roc_auc"],
    }
    assert api.metrics_show(rev="better-model~1") == {
        f"{train_metrics_file}:avg_prec": TRAIN_METRICS[1]["avg_prec"],
        f"{train_metrics_file}:roc_auc": TRAIN_METRICS[1]["roc_auc"],
        f"{test_metrics_file}:avg_prec": TEST_METRICS[0]["avg_prec"],
        f"{test_metrics_file}:roc_auc": TEST_METRICS[0]["roc_auc"],
    }
    assert api.metrics_show(rev="better-model") == {
        f"{train_metrics_file}:avg_prec": TRAIN_METRICS[1]["avg_prec"],
        f"{train_metrics_file}:roc_auc": TRAIN_METRICS[1]["roc_auc"],
        f"{test_metrics_file}:avg_prec": TEST_METRICS[1]["avg_prec"],
        f"{test_metrics_file}:roc_auc": TEST_METRICS[1]["roc_auc"],
    }


def test_metrics_show_dirty_working_dir(metrics_repo, tmp_dir):
    train_metrics_file, test_metrics_file = metrics_repo
    new_metrics = {"acc": 1}
    (tmp_dir / train_metrics_file).unlink()
    (tmp_dir / train_metrics_file).dump(new_metrics)
    (tmp_dir / test_metrics_file).unlink()
    (tmp_dir / test_metrics_file).dump(new_metrics)

    assert api.metrics_show() == {
        f"{train_metrics_file}:acc": new_metrics["acc"],
        f"{test_metrics_file}:acc": new_metrics["acc"],
    }
