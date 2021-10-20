import logging
import os

import pytest
from funcy import get_in

from dvc.dvcfile import PIPELINE_FILE
from dvc.exceptions import OverlappingOutputPathsError
from dvc.repo import Repo
from dvc.utils.fs import remove
from dvc.utils.serialize import YAMLFileCorruptedError


def test_show_simple(tmp_dir, dvc, run_copy_metrics):
    tmp_dir.gen("metrics_t.yaml", "1.1")
    run_copy_metrics(
        "metrics_t.yaml", "metrics.yaml", metrics=["metrics.yaml"]
    )
    assert dvc.metrics.show() == {
        "": {"data": {"metrics.yaml": {"data": 1.1}}}
    }


def test_show(tmp_dir, dvc, run_copy_metrics):
    tmp_dir.gen("metrics_t.yaml", "foo: 1.1")
    run_copy_metrics(
        "metrics_t.yaml", "metrics.yaml", metrics=["metrics.yaml"]
    )
    assert dvc.metrics.show() == {
        "": {"data": {"metrics.yaml": {"data": {"foo": 1.1}}}}
    }


def test_show_multiple(tmp_dir, dvc, run_copy_metrics):
    tmp_dir.gen("foo_temp", "foo: 1\n")
    tmp_dir.gen("baz_temp", "baz: 2\n")
    run_copy_metrics("foo_temp", "foo", fname="foo.dvc", metrics=["foo"])
    run_copy_metrics("baz_temp", "baz", fname="baz.dvc", metrics=["baz"])
    assert dvc.metrics.show() == {
        "": {
            "data": {"foo": {"data": {"foo": 1}}, "baz": {"data": {"baz": 2}}}
        }
    }


def test_show_branch(tmp_dir, scm, dvc, run_copy_metrics):
    tmp_dir.gen("metrics_temp.yaml", "foo: 1")
    run_copy_metrics(
        "metrics_temp.yaml", "metrics.yaml", metrics_no_cache=["metrics.yaml"]
    )
    scm.add(["metrics.yaml", "metrics.yaml.dvc"])
    scm.commit("init")

    with tmp_dir.branch("branch", new=True):
        tmp_dir.scm_gen("metrics.yaml", "foo: 2", commit="branch")

    assert dvc.metrics.show(revs=["branch"]) == {
        "workspace": {"data": {"metrics.yaml": {"data": {"foo": 1}}}},
        "branch": {"data": {"metrics.yaml": {"data": {"foo": 2}}}},
    }


def test_show_subrepo_with_preexisting_tags(tmp_dir, scm):
    tmp_dir.gen("foo", "foo")
    scm.add("foo")
    scm.commit("init")
    scm.tag("no-metrics")

    tmp_dir.gen({"subdir": {}})
    subrepo_dir = tmp_dir / "subdir"
    with subrepo_dir.chdir():
        dvc = Repo.init(subdir=True)
        scm.commit("init dvc")

        dvc.run(
            cmd="echo foo: 1 > metrics.yaml",
            metrics=["metrics.yaml"],
            single_stage=True,
        )

    scm.add(
        [
            str(subrepo_dir / "metrics.yaml"),
            str(subrepo_dir / "metrics.yaml.dvc"),
        ]
    )
    scm.commit("init metrics")
    scm.tag("v1")

    expected_path = os.path.join("subdir", "metrics.yaml")
    assert dvc.metrics.show(all_tags=True) == {
        "workspace": {"data": {expected_path: {"data": {"foo": 1}}}},
        "v1": {"data": {expected_path: {"data": {"foo": 1}}}},
    }


def test_missing_cache(tmp_dir, dvc, run_copy_metrics):
    tmp_dir.gen("metrics_t.yaml", "1.1")
    run_copy_metrics(
        "metrics_t.yaml", "metrics.yaml", metrics=["metrics.yaml"]
    )

    # This one should be skipped
    stage = run_copy_metrics(
        "metrics_t.yaml", "metrics2.yaml", metrics=["metrics2.yaml"]
    )
    remove(stage.outs[0].fspath)
    remove(stage.outs[0].cache_path)

    result = dvc.metrics.show()
    metrics2 = result[""]["data"].pop("metrics2.yaml")
    assert isinstance(metrics2["error"], FileNotFoundError)
    assert result == {
        "": {
            "data": {
                "metrics.yaml": {"data": 1.1},
            }
        }
    }


@pytest.mark.parametrize("use_dvc", [True, False])
def test_show_non_metric(tmp_dir, scm, use_dvc):
    tmp_dir.gen("metrics.yaml", "foo: 1.1")

    if use_dvc:
        dvc = Repo.init()
    else:
        dvc = Repo(uninitialized=True)

    assert dvc.metrics.show(targets=["metrics.yaml"]) == {
        "": {"data": {"metrics.yaml": {"data": {"foo": 1.1}}}}
    }

    if not use_dvc:
        assert not (tmp_dir / ".dvc").exists()


@pytest.mark.parametrize("use_dvc", [True, False])
def test_show_non_metric_branch(tmp_dir, scm, use_dvc):
    tmp_dir.scm_gen("metrics.yaml", "foo: 1.1", commit="init")
    with tmp_dir.branch("branch", new=True):
        tmp_dir.scm_gen("metrics.yaml", "foo: 2.2", commit="other")

    if use_dvc:
        dvc = Repo.init()
    else:
        dvc = Repo(uninitialized=True)

    assert dvc.metrics.show(targets=["metrics.yaml"], revs=["branch"]) == {
        "workspace": {"data": {"metrics.yaml": {"data": {"foo": 1.1}}}},
        "branch": {"data": {"metrics.yaml": {"data": {"foo": 2.2}}}},
    }

    if not use_dvc:
        assert not (tmp_dir / ".dvc").exists()


def test_non_metric_and_recurisve_show(tmp_dir, dvc, run_copy_metrics):
    tmp_dir.gen(
        {"metrics_t.yaml": "foo: 1.1", "metrics": {"metric1.yaml": "bar: 1.2"}}
    )

    metric2 = os.fspath(tmp_dir / "metrics" / "metric2.yaml")
    run_copy_metrics("metrics_t.yaml", metric2, metrics=[metric2])

    assert dvc.metrics.show(
        targets=["metrics_t.yaml", "metrics"], recursive=True
    ) == {
        "": {
            "data": {
                os.path.join("metrics", "metric1.yaml"): {
                    "data": {"bar": 1.2}
                },
                os.path.join("metrics", "metric2.yaml"): {
                    "data": {"foo": 1.1}
                },
                "metrics_t.yaml": {"data": {"foo": 1.1}},
            }
        }
    }


def test_show_falsey(tmp_dir, dvc):
    tmp_dir.gen("metrics.json", '{"foo": 0, "bar": 0.0, "baz": {}}')
    assert dvc.metrics.show(targets=["metrics.json"]) == {
        "": {"data": {"metrics.json": {"data": {"foo": 0, "bar": 0.0}}}}
    }


def test_show_no_repo(tmp_dir):
    tmp_dir.gen("metrics.json", '{"foo": 0, "bar": 0.0, "baz": {}}')

    dvc = Repo(uninitialized=True)

    assert dvc.metrics.show(targets=["metrics.json"]) == {
        "": {"data": {"metrics.json": {"data": {"foo": 0, "bar": 0.0}}}}
    }


def test_show_malformed_metric(tmp_dir, scm, dvc, caplog):
    tmp_dir.gen("metric.json", '{"m":1')

    assert isinstance(
        dvc.metrics.show(targets=["metric.json"])[""]["data"]["metric.json"][
            "error"
        ],
        YAMLFileCorruptedError,
    )


def test_metrics_show_no_target(tmp_dir, dvc, caplog):
    with caplog.at_level(logging.WARNING):
        assert dvc.metrics.show(targets=["metrics.json"]) == {"": {}}

    assert (
        "'metrics.json' was not found in current workspace." in caplog.messages
    )


def test_show_no_metrics_files(tmp_dir, dvc, caplog):
    assert dvc.metrics.show() == {"": {}}


@pytest.mark.parametrize("clear_before_run", [True, False])
def test_metrics_show_overlap(
    tmp_dir, dvc, run_copy_metrics, clear_before_run
):
    data_dir = tmp_dir / "data"
    data_dir.mkdir()

    (data_dir / "m1_temp.yaml").dump({"a": {"b": {"c": 2, "d": 1}}})
    run_copy_metrics(
        str(data_dir / "m1_temp.yaml"),
        str(data_dir / "m1.yaml"),
        single_stage=False,
        commit="add m1",
        name="cp-m1",
        metrics=[str(data_dir / "m1.yaml")],
    )
    with (tmp_dir / "dvc.yaml").modify() as d:
        # trying to make an output overlaps error
        d["stages"]["corrupted-stage"] = {
            "cmd": "mkdir data",
            "outs": ["data"],
        }

    # running by clearing and not clearing stuffs
    # so as it works even for optimized cases
    if clear_before_run:
        remove(data_dir)
        remove(dvc.odb.local.cache_dir)

    dvc._reset()

    res = dvc.metrics.show()
    assert isinstance(res[""]["error"], OverlappingOutputPathsError)


@pytest.mark.parametrize(
    "file,error_path",
    (
        (PIPELINE_FILE, ["workspace", "error"]),
        ("metrics.yaml", ["workspace", "data", "metrics.yaml", "error"]),
    ),
)
def test_log_errors(
    tmp_dir, scm, dvc, capsys, run_copy_metrics, file, error_path
):
    tmp_dir.gen("metrics_t.yaml", "m: 1.1")
    run_copy_metrics(
        "metrics_t.yaml",
        "metrics.yaml",
        metrics=["metrics.yaml"],
        single_stage=False,
        name="train",
    )
    scm.tag("v1")

    with open(file, "a") as fd:
        fd.write("\nMALFORMED!")

    result = dvc.metrics.show(revs=["v1"])

    _, error = capsys.readouterr()

    assert isinstance(get_in(result, error_path), YAMLFileCorruptedError)
    assert (
        "DVC failed to load some metrics for following revisions: 'workspace'."
        in error
    )
