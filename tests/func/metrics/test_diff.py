import json
from os.path import join

import pytest

from dvc.cli import main
from dvc.utils import relpath
from dvc.utils.serialize import JSONFileCorruptedError


def test_metrics_diff_simple(tmp_dir, scm, dvc, run_copy_metrics):
    def _gen(val):
        tmp_dir.gen({"m_temp.yaml": str(val)})
        run_copy_metrics(
            "m_temp.yaml", "m.yaml", name="copy-metrics", metrics=["m.yaml"]
        )
        dvc.scm.commit(str(val))

    _gen(1)
    _gen(2)
    _gen(3)

    expected = {"m.yaml": {"": {"old": 1, "new": 3, "diff": 2}}}

    assert dvc.metrics.diff(a_rev="HEAD~2") == {"diff": expected}


def test_metrics_diff_yaml(tmp_dir, scm, dvc, run_copy_metrics):
    def _gen(val):
        metrics = {"a": {"b": {"c": val, "d": 1, "e": str(val)}}}
        (tmp_dir / "m_temp.yaml").dump(metrics)
        run_copy_metrics(
            "m_temp.yaml",
            "m.yaml",
            name="copy-metrics",
            metrics=["m.yaml"],
            commit=str(val),
        )

    _gen(1)
    _gen(2)
    _gen(3)

    expected = {
        "m.yaml": {
            "a.b.e": {"old": "1", "new": "3"},
            "a.b.c": {"old": 1, "new": 3, "diff": 2},
        }
    }

    assert dvc.metrics.diff(a_rev="HEAD~2") == {"diff": expected}


def test_metrics_diff_json(tmp_dir, scm, dvc, run_copy_metrics):
    def _gen(val):
        metrics = {"a": {"b": {"c": val, "d": 1, "e": str(val)}}}
        (tmp_dir / "m_temp.json").dump(metrics)
        run_copy_metrics(
            "m_temp.json",
            "m.json",
            name="copy-metrics",
            metrics=["m.json"],
            commit=str(val),
        )

    _gen(1)
    _gen(2)
    _gen(3)

    expected = {
        "m.json": {
            "a.b.e": {"old": "1", "new": "3"},
            "a.b.c": {"old": 1, "new": 3, "diff": 2},
        }
    }
    assert dvc.metrics.diff(a_rev="HEAD~2") == {"diff": expected}


def test_metrics_diff_json_unchanged(tmp_dir, scm, dvc, run_copy_metrics):
    def _gen(val):
        metrics = {"a": {"b": {"c": val, "d": 1, "e": str(val)}}}
        (tmp_dir / "m_temp.json").dump(metrics)
        run_copy_metrics(
            "m_temp.json",
            "m.json",
            name="copy-metrics",
            metrics=["m.json"],
            commit=str(val),
        )

    _gen(1)
    _gen(2)
    _gen(1)

    assert dvc.metrics.diff(a_rev="HEAD~2") == {}


def test_metrics_diff_broken_json(M, tmp_dir, scm, dvc, run_copy_metrics):
    metrics = {"a": {"b": {"c": 1, "d": 1, "e": "3"}}}
    (tmp_dir / "m_temp.json").dump(metrics)
    run_copy_metrics(
        "m_temp.json",
        "m.json",
        name="copy-metrics",
        metrics_no_cache=["m.json"],
        commit="add metrics",
    )

    (tmp_dir / "m.json").write_text(json.dumps(metrics) + "ma\nlformed\n")

    assert dvc.metrics.diff() == {
        "diff": {
            "m.json": {
                "a.b.e": {"old": "3", "new": None},
                "a.b.c": {"old": 1, "new": None},
                "a.b.d": {"old": 1, "new": None},
            }
        },
        "errors": {"workspace": {"m.json": M.instance_of(JSONFileCorruptedError)}},
    }


def test_metrics_diff_no_metrics(tmp_dir, scm, dvc):
    tmp_dir.scm_gen({"foo": "foo"}, commit="add foo")
    assert dvc.metrics.diff(a_rev="HEAD~1") == {}


def test_metrics_diff_new_metric(tmp_dir, scm, dvc, run_copy_metrics):
    metrics = {"a": {"b": {"c": 1, "d": 1, "e": "3"}}}
    (tmp_dir / "m_temp.json").dump(metrics)
    run_copy_metrics(
        "m_temp.json", "m.json", name="copy-metrics", metrics_no_cache=["m.json"]
    )

    assert dvc.metrics.diff() == {
        "diff": {
            "m.json": {
                "a.b.e": {"old": None, "new": "3"},
                "a.b.c": {"old": None, "new": 1},
                "a.b.d": {"old": None, "new": 1},
            }
        }
    }


def test_metrics_diff_deleted_metric(M, tmp_dir, scm, dvc, run_copy_metrics):
    metrics = {"a": {"b": {"c": 1, "d": 1, "e": "3"}}}
    (tmp_dir / "m_temp.json").dump(metrics)
    run_copy_metrics(
        "m_temp.json",
        "m.json",
        name="copy-metrics",
        metrics_no_cache=["m.json"],
        commit="add metrics",
    )

    (tmp_dir / "m.json").unlink()

    assert dvc.metrics.diff() == {
        "diff": {
            "m.json": {
                "a.b.e": {"old": "3", "new": None},
                "a.b.c": {"old": 1, "new": None},
                "a.b.d": {"old": 1, "new": None},
            }
        },
        "errors": {"workspace": {"m.json": M.instance_of(FileNotFoundError)}},
    }


def test_metrics_diff_with_unchanged(tmp_dir, scm, dvc, run_copy_metrics):
    tmp_dir.gen("metrics_temp.yaml", "foo: 1\nxyz: 10")
    run_copy_metrics(
        "metrics_temp.yaml",
        "metrics.yaml",
        name="copy-metrics",
        metrics_no_cache=["metrics.yaml"],
        commit="1",
    )

    tmp_dir.scm_gen("metrics.yaml", "foo: 2\nxyz: 10", commit="2")
    tmp_dir.scm_gen("metrics.yaml", "foo: 3\nxyz: 10", commit="3")

    assert dvc.metrics.diff(a_rev="HEAD~2", all=True) == {
        "diff": {
            "metrics.yaml": {
                "foo": {"old": 1, "new": 3, "diff": 2},
                "xyz": {"old": 10, "new": 10, "diff": 0},
            }
        }
    }


def test_no_commits(tmp_dir):
    from dvc.repo import Repo
    from dvc.scm import Git

    git = Git.init(tmp_dir.fs_path)
    assert git.no_commits

    assert Repo.init().metrics.diff() == {}


def test_metrics_diff_dirty(tmp_dir, scm, dvc, run_copy_metrics):
    def _gen(val):
        tmp_dir.gen({"m_temp.yaml": str(val)})
        run_copy_metrics(
            "m_temp.yaml", "m.yaml", name="copy-metrics", metrics=["m.yaml"]
        )
        dvc.scm.commit(str(val))

    _gen(1)
    _gen(2)
    _gen(3)

    tmp_dir.gen({"m.yaml": "4"})

    expected = {"m.yaml": {"": {"old": 3, "new": 4, "diff": 1}}}

    assert dvc.metrics.diff() == {"diff": expected}


def test_metrics_diff_cli(tmp_dir, scm, dvc, run_copy_metrics, caplog, capsys):
    def _gen(val):
        tmp_dir.gen({"m_temp.yaml": f"foo: {val}"})
        run_copy_metrics(
            "m_temp.yaml", "m.yaml", name="copy-metrics", metrics=["m.yaml"]
        )
        dvc.scm.commit(str(val))

    _gen(1.23456789)
    _gen(2.34567891011)
    _gen(3.45678910111213)

    caplog.clear()
    capsys.readouterr()  # clearing the buffer
    assert main(["metrics", "diff", "HEAD~2"]) == 0

    captured = capsys.readouterr()

    assert (
        captured.out == "Path    Metric    HEAD~2    workspace    Change\n"
        "m.yaml  foo       1.23457   3.45679      2.22222\n"
    )


def test_metrics_diff_non_metrics(tmp_dir, scm, dvc):
    def _gen(val):
        tmp_dir.scm_gen({"some_file.yaml": f"foo: {val}"}, commit=str(val))

    _gen(1)
    _gen(2)
    _gen(3)

    result = dvc.metrics.diff(targets=["some_file.yaml"], a_rev="HEAD~2")
    assert result == {
        "diff": {"some_file.yaml": {"foo": {"old": 1, "new": 3, "diff": 2}}}
    }


@pytest.mark.parametrize(
    "dvcfile, metrics_file",
    [
        ("dvc.yaml", "my_metrics.yaml"),
        ("dir/dvc.yaml", "my_metrics.yaml"),
        ("dir/dvc.yaml", join("..", "my_metrics.yaml")),
    ],
)
def test_diff_top_level_metrics(tmp_dir, dvc, scm, dvcfile, metrics_file):
    directory = (tmp_dir / dvcfile).parent
    directory.mkdir(exist_ok=True)
    (tmp_dir / dvcfile).dump({"metrics": [metrics_file]})

    metrics_file = directory / metrics_file
    metrics_file.dump({"foo": 3})
    scm.add_commit([metrics_file, tmp_dir / dvcfile], message="add metrics")

    metrics_file.dump({"foo": 5})
    assert dvc.metrics.diff() == {
        "diff": {
            relpath(directory / metrics_file): {"foo": {"diff": 2, "new": 5, "old": 3}}
        }
    }


def test_metrics_diff_active_branch_unchanged(tmp_dir, scm, dvc, run_copy_metrics):
    def _gen(val):
        metrics = {"a": {"b": {"c": val, "d": 1, "e": str(val)}}}
        (tmp_dir / "m_temp.yaml").dump(metrics)
        run_copy_metrics(
            "m_temp.yaml",
            "m.yaml",
            name="copy-metric",
            metrics=["m.yaml"],
            commit=str(val),
        )

    _gen(1)
    _gen(2)
    _gen(1)

    assert dvc.metrics.diff(a_rev=tmp_dir.scm.active_branch()) == {}
