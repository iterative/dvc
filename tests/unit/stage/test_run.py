import logging

from dvc.stage import Stage
from dvc.stage.run import run_stage


def test_run_stage_dry(caplog):
    with caplog.at_level(level=logging.INFO, logger="dvc"):
        run_stage(Stage(None, cmd="mycmd arg1 arg2"), dry=True)
        assert caplog.messages == ["Running command:\n\tmycmd arg1 arg2"]
