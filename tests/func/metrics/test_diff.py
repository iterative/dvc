import json
import yaml


def test_metrics_diff_simple(tmp_dir, scm, dvc):
    def _gen(val):
        tmp_dir.gen({"m.yaml": str(val)})
        dvc.run(cmd="", metrics=["m.yaml"], single_stage=True)
        dvc.scm.add(["m.yaml.dvc"])
        dvc.scm.commit(str(val))

    _gen(1)
    _gen(2)
    _gen(3)

    expected = {"m.yaml": {"": {"old": 1, "new": 3, "diff": 2}}}

    assert dvc.metrics.diff(a_rev="HEAD~2") == expected


def test_metrics_diff_yaml(tmp_dir, scm, dvc):
    def _gen(val):
        metrics = {"a": {"b": {"c": val, "d": 1, "e": str(val)}}}
        tmp_dir.gen({"m.yaml": yaml.dump(metrics)})
        dvc.run(cmd="", metrics=["m.yaml"], single_stage=True)
        dvc.scm.add(["m.yaml.dvc"])
        dvc.scm.commit(str(val))

    _gen(1)
    _gen(2)
    _gen(3)

    expected = {"m.yaml": {"a.b.c": {"old": 1, "new": 3, "diff": 2}}}

    assert dvc.metrics.diff(a_rev="HEAD~2") == expected


def test_metrics_diff_json(tmp_dir, scm, dvc):
    def _gen(val):
        metrics = {"a": {"b": {"c": val, "d": 1, "e": str(val)}}}
        tmp_dir.gen({"m.json": json.dumps(metrics)})
        dvc.run(cmd="", metrics=["m.json"], single_stage=True)
        dvc.scm.add(["m.json.dvc"])
        dvc.scm.commit(str(val))

    _gen(1)
    _gen(2)
    _gen(3)

    expected = {"m.json": {"a.b.c": {"old": 1, "new": 3, "diff": 2}}}
    assert dvc.metrics.diff(a_rev="HEAD~2") == expected


def test_metrics_diff_json_unchanged(tmp_dir, scm, dvc):
    def _gen(val):
        metrics = {"a": {"b": {"c": val, "d": 1, "e": str(val)}}}
        tmp_dir.gen({"m.json": json.dumps(metrics)})
        dvc.run(cmd="", metrics=["m.json"], single_stage=True)
        dvc.scm.add(["m.json.dvc"])
        dvc.scm.commit(str(val))

    _gen(1)
    _gen(2)
    _gen(1)

    assert dvc.metrics.diff(a_rev="HEAD~2") == {}


def test_metrics_diff_broken_json(tmp_dir, scm, dvc):
    metrics = {"a": {"b": {"c": 1, "d": 1, "e": "3"}}}
    tmp_dir.gen({"m.json": json.dumps(metrics)})
    dvc.run(cmd="", metrics_no_cache=["m.json"], single_stage=True)
    dvc.scm.add(["m.json.dvc", "m.json"])
    dvc.scm.commit("add metrics")

    (tmp_dir / "m.json").write_text(json.dumps(metrics) + "ma\nlformed\n")

    assert dvc.metrics.diff() == {
        "m.json": {
            "a.b.c": {"old": 1, "new": None},
            "a.b.d": {"old": 1, "new": None},
        }
    }


def test_metrics_diff_no_metrics(tmp_dir, scm, dvc):
    tmp_dir.scm_gen({"foo": "foo"}, commit="add foo")
    assert dvc.metrics.diff(a_rev="HEAD~1") == {}


def test_metrics_diff_new_metric(tmp_dir, scm, dvc):
    metrics = {"a": {"b": {"c": 1, "d": 1, "e": "3"}}}
    tmp_dir.gen({"m.json": json.dumps(metrics)})
    dvc.run(cmd="", metrics_no_cache=["m.json"], single_stage=True)

    assert dvc.metrics.diff() == {
        "m.json": {
            "a.b.c": {"old": None, "new": 1},
            "a.b.d": {"old": None, "new": 1},
        }
    }


def test_metrics_diff_deleted_metric(tmp_dir, scm, dvc):
    metrics = {"a": {"b": {"c": 1, "d": 1, "e": "3"}}}
    tmp_dir.gen({"m.json": json.dumps(metrics)})
    dvc.run(cmd="", metrics_no_cache=["m.json"], single_stage=True)
    dvc.scm.add(["m.json.dvc", "m.json"])
    dvc.scm.commit("add metrics")

    (tmp_dir / "m.json").unlink()

    assert dvc.metrics.diff() == {
        "m.json": {
            "a.b.c": {"old": 1, "new": None},
            "a.b.d": {"old": 1, "new": None},
        }
    }


def test_metrics_diff_with_unchanged(tmp_dir, scm, dvc):
    tmp_dir.gen("metrics.yaml", "foo: 1\nxyz: 10")
    dvc.run(metrics_no_cache=["metrics.yaml"], single_stage=True)
    scm.add(["metrics.yaml", "metrics.yaml.dvc"])
    scm.commit("1")

    tmp_dir.scm_gen("metrics.yaml", "foo: 2\nxyz: 10", commit="2")
    tmp_dir.scm_gen("metrics.yaml", "foo: 3\nxyz: 10", commit="3")

    assert dvc.metrics.diff(a_rev="HEAD~2", all=True) == {
        "metrics.yaml": {
            "foo": {"old": 1, "new": 3, "diff": 2},
            "xyz": {"old": 10, "new": 10, "diff": 0},
        }
    }
