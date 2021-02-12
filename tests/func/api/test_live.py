import io
import json
import logging
from typing import Dict, List

import pytest
from funcy import last

from dvc.api.live import summary
from tests.utils import dump_sv


def _dumps_tsv(metrics: List[Dict]):
    stream = io.StringIO()
    dump_sv(stream, metrics)
    stream.seek(0)
    return stream.read()


@pytest.fixture
def live_results(tmp_dir):
    def make(path="logs"):
        datapoints = [{"metric": 0.0, "step": 0}, {"metric": 0.5, "step": 1}]
        tmp_dir.gen(
            {
                (tmp_dir / path).with_suffix(".json"): json.dumps(
                    last(datapoints)
                ),
                (tmp_dir / path / "metric.tsv"): _dumps_tsv(datapoints),
            }
        )

    yield make


def test_live_summary_no_repo(tmp_dir, live_results, caplog):
    live_results("logs")

    with caplog.at_level(logging.INFO, logger="dvc"):
        summary("logs")

    summary_path = tmp_dir / "logs.html"
    assert summary_path.exists()
    assert f"file://{str(summary_path)}" in caplog.text


def test_live_summary(tmp_dir, dvc, live_results, caplog):
    live_results("logs")

    with caplog.at_level(logging.INFO, logger="dvc"):
        summary("logs")

    summary_path = tmp_dir / "logs.html"
    assert summary_path.exists()
    assert f"file://{str(summary_path)}" in caplog.text
