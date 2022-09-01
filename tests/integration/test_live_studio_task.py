from textwrap import dedent

import pytest
from flaky.flaky_decorator import flaky

LIVE_SCRIPT = dedent(
    """
        import time
        from dvclive import Live
        from PIL import Image

        import sys
        r = 2
        metrics_logger = Live(report=None)
        for i in range(r):
           metrics_logger.log("loss", 1-i/r)
           # Not indicated in DVC.YAML will be filtered out
           metrics_logger.log("accuracy", i/r)
           metrics_logger.next_step()
           # Longer than Monitor.AWAIT
           time.sleep(0.2)
    """
)


@pytest.fixture
def live_studio_stage(tmp_dir, dvc, scm):
    tmp_dir.gen("train.py", LIVE_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")
    stage = dvc.run(
        cmd="python train.py",
        params=["foo"],
        deps=["train.py"],
        name="live_stage",
        metrics_no_cache=["dvclive.json"],
        plots_no_cache=["dvclive/scalars/loss.tsv"],
    )

    scm.add(["dvc.yaml", "dvc.lock", "train.py", "params.yaml"])
    scm.commit("initial: live_stage")
    return stage


# TODO: Find a non-flaky way of testing this
@pytest.mark.studio
@flaky(max_runs=3, min_passes=1)
def test_live_studio_task(live_studio_stage, dvc, scm, mocker, monkeypatch):
    from dvc.version import __version__

    monkeypatch.setenv("DVC_STUDIO_URL", "https://studio.iterative.ai/foo")
    mocker.patch("dvc.stage.run.Monitor.AWAIT", 0.1)

    mock_session = mocker.MagicMock()
    mocker.patch("requests.Session", return_value=mock_session)

    dvc.experiments.run(force=True)
    assert mock_session.post.call_count == 3
    assert mock_session.post.call_args[1] == {
        "json": {
            "version": __version__,
            "rev": scm.get_rev(),
            "metrics": {
                "dvclive.json": {"step": 1, "loss": 0.5, "accuracy": 0.5}
            },
            "plots": {
                "dvclive/scalars/loss.tsv": [
                    {"timestamp": mocker.ANY, "step": "1", "loss": "0.5"}
                ]
            },
        }
    }
