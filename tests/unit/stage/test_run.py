import logging

from dvc.stage import Stage
from dvc.stage.run import run_stage
from dvc.utils import styled


def test_run_stage_dry(caplog):
    with caplog.at_level(level=logging.INFO, logger="dvc"):
        stage = Stage(None, "stage.dvc", cmd="mycmd arg1 arg2")
        run_stage(stage, dry=True)
        assert caplog.messages == [
            "Running {} stage {} with command:".format(
                styled("callback", "bold"), styled("stage.dvc", "bold")
            ),
            "\t" + "mycmd arg1 arg2",
        ]
