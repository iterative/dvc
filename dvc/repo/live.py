import contextlib
import os
from typing import List

from dvc.exceptions import NoMetricsError
from dvc.output import BaseOutput
from dvc.path_info import PathInfo
from dvc.repo import Repo


def summary_path_info(out: BaseOutput) -> PathInfo:
    assert out.dvclive
    has_summary = True
    if isinstance(out.dvclive, dict):
        has_summary = out.dvclive.get(BaseOutput.PARAM_LIVE_SUMMARY, True)
    if has_summary:
        return out.path_info.with_suffix(".json")
    return None


class DvcLive:
    def __init__(self, repo: Repo):
        self.repo = repo

    def show(self, path: str, revs: List[str] = None):
        if revs:
            revs = ["workspace", *revs]

        assert os.path.exists(path)

        metrics_path = path + ".json"

        metrics = None
        with contextlib.suppress(NoMetricsError):
            metrics = self.repo.metrics.show(targets=[metrics_path])

        plots = self.repo.plots.show(path, recursive=True, revs=revs)

        return metrics, plots
