from textwrap import dedent

import pytest

from dvc import stage as stage_module
from dvc.exceptions import MetricsError

LIVE_SCRITP = dedent(
    """
        import dvclive
        import sys
        r = 2
        for i in range(r):
           dvclive.log("loss", 1-i/r)
           dvclive.log("accuracy", i/r)
           dvclive.next_step()"""
)


@pytest.fixture
def live_stage(tmp_dir, scm, dvc):

    pytest.skip("dvclive does not exist yet")

    def make(summary=True, report=True):
        tmp_dir.gen("train.py", LIVE_SCRITP)
        tmp_dir.gen("params.yaml", "foo: 1")
        stage = dvc.run(
            cmd="python train.py",
            params=["foo"],
            deps=["train.py"],
            name="live_stage",
            live="logs",
            live_summary=summary,
            live_report=report,
        )

        scm.add(["dvc.yaml", "dvc.lock", "train.py", "params.yaml"])
        scm.commit("initial: live_stage")
        return stage

    yield make


@pytest.mark.parametrize("report", (True, False))
@pytest.mark.parametrize("summary", (True, False))
def test_export_config_tmp(tmp_dir, dvc, mocker, summary, report):
    run_spy = mocker.spy(stage_module.run, "_run")
    tmp_dir.gen("src", "dependency")
    dvc.run(
        cmd="mkdir logs && touch logs.json",
        deps=["src"],
        name="run_logger",
        live="logs",
        live_summary=summary,
        live_report=report,
    )

    assert run_spy.call_count == 1
    _, kwargs = run_spy.call_args

    assert "DVCLIVE_PATH" in kwargs["env"]
    assert kwargs["env"]["DVCLIVE_PATH"] == "logs"

    assert "DVCLIVE_SUMMARY" in kwargs["env"]
    assert kwargs["env"]["DVCLIVE_SUMMARY"] == str(int(summary))

    assert "DVCLIVE_REPORT" in kwargs["env"]
    assert kwargs["env"]["DVCLIVE_REPORT"] == str(int(report))


@pytest.mark.parametrize("summary", (True, False))
def test_export_config(tmp_dir, dvc, mocker, summary, live_stage):
    run_spy = mocker.spy(stage_module.run, "_run")
    live_stage(summary=summary)

    assert run_spy.call_count == 1
    _, kwargs = run_spy.call_args

    assert "DVCLIVE_PATH" in kwargs["env"]
    assert kwargs["env"]["DVCLIVE_PATH"] == "logs"

    assert "DVCLIVE_SUMMARY" in kwargs["env"]
    assert kwargs["env"]["DVCLIVE_SUMMARY"] == str(int(summary))


def test_live_provides_metrics(tmp_dir, dvc, live_stage):
    live_stage(summary=True)

    assert (tmp_dir / "logs.json").is_file()
    assert dvc.metrics.show() == {
        "": {"logs.json": {"step": 1, "loss": 0.5, "accuracy": 0.5}}
    }

    assert (tmp_dir / "logs").is_dir()
    plots = dvc.plots.show()
    assert "logs/accuracy.tsv" in plots
    assert "logs/loss.tsv" in plots


def test_live_provides_no_metrics(tmp_dir, dvc, live_stage):
    live_stage(summary=False)

    assert not (tmp_dir / "logs.json").is_file()
    with pytest.raises(MetricsError):
        assert dvc.metrics.show() == {}

    assert (tmp_dir / "logs").is_dir()
    plots = dvc.plots.show()
    assert "logs/accuracy.tsv" in plots
    assert "logs/loss.tsv" in plots


def test_experiments_track_summary(tmp_dir, scm, dvc, live_stage):
    live_stage(summary=True)
    baseline_rev = scm.get_rev()

    experiments = dvc.experiments.run(targets=["live_stage"], params=["foo=2"])
    assert len(experiments) == 1
    ((exp_rev, _),) = experiments.items()

    res = dvc.experiments.show()
    assert "logs.json" in res[baseline_rev][exp_rev]["metrics"].keys()


@pytest.mark.parametrize("report", [True, False])
def test_live_report(tmp_dir, dvc, live_stage, report):
    live_stage(report=report)

    assert (tmp_dir / "logs.html").is_file() == report
