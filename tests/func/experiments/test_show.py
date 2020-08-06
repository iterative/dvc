from tests.func.test_repro_multistage import COPY_SCRIPT


def test_show_simple(tmp_dir, scm, dvc):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")
    dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        single_stage=True,
    )

    assert dvc.experiments.show()["workspace"] == {
        "baseline": {
            "metrics": {"metrics.yaml": {"foo": 1}},
            "params": {"params.yaml": {"foo": 1}},
            "queued": False,
        }
    }
