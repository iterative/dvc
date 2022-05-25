import os
from copy import deepcopy
from textwrap import dedent

import pytest
from funcy import first

from dvc import stage as stage_module

pytest.importorskip("dvclive", reason="no dvclive")


LIVE_SCRIPT = dedent(
    """
        from dvclive import Live
        from PIL import Image

        import sys
        r = 2
        metrics_logger = Live()
        for i in range(r):
           metrics_logger.log("loss", 1-i/r)
           metrics_logger.log("accuracy", i/r)
           image = Image.new("RGB", (10,10), (i,i,i))
           metrics_logger.log_image("image.jpg", image)
           metrics_logger.next_step()"""
)

LIVE_CHECKPOINT_SCRIPT = dedent(
    """
            import os
            from dvclive import Live

            def read(path):
                value=0
                if os.path.exists(path):
                    with open(path, 'r') as fobj:
                        try:
                            value = int(fobj.read())
                        except ValueError:
                            pass
                return value

            def dump(value, path):
                with open(path, "w") as fobj:
                    fobj.write(str(value))

            r = 3
            checkpoint_file = "checkpoint"
            metrics_logger = Live()

            value = read(checkpoint_file)
            for i in range(1,r):
                m = i + value
                dump(m, checkpoint_file)

                metrics_logger.log("metric1", m)
                metrics_logger.log("metric2", m * 2)
                metrics_logger.next_step()"""
)


@pytest.fixture
def live_stage(tmp_dir, scm, dvc, mocker):
    mocker.patch("dvc.stage.run.Monitor.AWAIT", 0.01)

    def make(
        html=True,
        live=None,
        live_no_cache=None,
        code=LIVE_SCRIPT,
    ):
        assert bool(live) != bool(live_no_cache)
        tmp_dir.gen("train.py", code)
        tmp_dir.gen("params.yaml", "foo: 1")
        stage = dvc.run(
            cmd="python train.py",
            params=["foo"],
            deps=["train.py"],
            name="live_stage",
            live=live,
            live_no_cache=live_no_cache,
            live_no_html=not html,
        )

        scm.add(["dvc.yaml", "dvc.lock", "train.py", "params.yaml"])
        scm.commit("initial: live_stage")
        return stage

    yield make


@pytest.mark.parametrize("html", (True, False))
def test_export_config(tmp_dir, dvc, mocker, live_stage, html):
    run_spy = mocker.spy(stage_module.run, "_run")
    live_stage(html=html, live="logs")

    assert run_spy.call_count == 1
    _, kwargs = run_spy.call_args

    assert "DVCLIVE_PATH" in kwargs["env"]
    assert kwargs["env"]["DVCLIVE_PATH"] == "logs"

    assert "DVCLIVE_HTML" in kwargs["env"]
    assert kwargs["env"]["DVCLIVE_HTML"] == str(int(html))


def test_live_provides_metrics(tmp_dir, dvc, live_stage):
    live_stage(live="logs")

    assert (tmp_dir / "logs.json").is_file()
    assert dvc.metrics.show() == {
        "": {
            "data": {
                "logs.json": {
                    "data": {"accuracy": 0.5, "loss": 0.5, "step": 1}
                }
            }
        }
    }

    assert (tmp_dir / "logs").is_dir()
    plots_data = dvc.plots.show()
    files = list(plots_data["workspace"]["data"])
    assert os.path.join("logs", "scalars", "accuracy.tsv") in files
    assert os.path.join("logs", "scalars", "loss.tsv") in files
    assert os.path.join("logs", "images", "0", "image.jpg") in files
    assert os.path.join("logs", "images", "1", "image.jpg") in files


@pytest.mark.parametrize("typ", ("live", "live_no_cache"))
def test_experiments_track_summary(tmp_dir, scm, dvc, live_stage, typ):
    live_stage(**{typ: "logs"})
    baseline_rev = scm.get_rev()

    experiments = dvc.experiments.run(targets=["live_stage"], params=["foo=2"])
    assert len(experiments) == 1
    ((exp_rev, _),) = experiments.items()

    res = dvc.experiments.show()
    assert "logs.json" in res[baseline_rev][exp_rev]["data"]["metrics"].keys()


@pytest.mark.parametrize("html", [True, False])
def test_live_html(tmp_dir, dvc, live_stage, html):
    live_stage(live="logs", html=html)

    assert (tmp_dir / "logs_dvc_plots" / "index.html").is_file() == html
    if html:
        html_text = (tmp_dir / "logs_dvc_plots" / "index.html").read_text()
        assert 'http-equiv="refresh"' in html_text


@pytest.fixture
def live_checkpoint_stage(tmp_dir, scm, dvc, mocker):
    mocker.patch("dvc.stage.run.Monitor.AWAIT", 0.01)

    def make(live=None, live_no_cache=None):
        assert bool(live) != bool(live_no_cache)

        tmp_dir.gen("train.py", LIVE_CHECKPOINT_SCRIPT)
        tmp_dir.gen("params.yaml", "foo: 1")
        stage = dvc.run(
            cmd="python train.py",
            params=["foo"],
            deps=["train.py"],
            name="live_stage",
            live=live,
            live_no_cache=live_no_cache,
            checkpoints=["checkpoint"],
            no_exec=True,
        )

        scm.add(["dvc.yaml", "train.py", "params.yaml", ".gitignore"])
        scm.commit("initial: live_stage")
        return stage

    yield make


def checkpoints_metric(show_results, metric_file, metric_name):
    tmp = deepcopy(show_results)
    tmp.pop("workspace")
    tmp = first(tmp.values())
    tmp.pop("baseline")
    return [
        exp["data"]["metrics"][metric_file]["data"][metric_name]
        for exp in tmp.values()
    ]


@pytest.mark.parametrize("typ", ("live", "live_no_cache"))
def test_live_checkpoints_resume(
    tmp_dir, scm, dvc, live_checkpoint_stage, typ
):
    stage = live_checkpoint_stage(**{typ: "logs"})
    results = dvc.experiments.run(
        stage.addressing, params=["foo=2"], tmp_dir=False
    )

    checkpoint_resume = first(results)

    dvc.experiments.run(
        stage.addressing, checkpoint_resume=checkpoint_resume, tmp_dir=False
    )

    results = dvc.experiments.show()
    assert checkpoints_metric(results, "logs.json", "step") == [3, 2, 1, 0]
    assert checkpoints_metric(results, "logs.json", "metric1") == [4, 3, 2, 1]
    assert checkpoints_metric(results, "logs.json", "metric2") == [8, 6, 4, 2]


def test_dvclive_stage_with_different_wdir(tmp_dir, scm, dvc):
    (tmp_dir / "subdir").gen("train.py", LIVE_SCRIPT)
    (tmp_dir / "subdir" / "params.yaml").dump({"foo": 1})
    dvc.stage.add(
        cmd="python train.py",
        params=["foo"],
        deps=["train.py"],
        name="live_stage",
        live="dvclive",
        live_no_html=True,
        wdir="subdir",
    )

    dvc.reproduce()

    assert (tmp_dir / "subdir" / "dvclive").is_dir()
    assert (tmp_dir / "subdir" / "dvclive.json").is_file()
