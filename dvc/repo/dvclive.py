import contextlib
import logging
import os
from typing import List

from dvc.exceptions import NoMetricsError
from dvc.output import BaseOutput
from dvc.path_info import PathInfo
from dvc.repo import Repo
from dvc.visualization import embed, metrics_embedding, plots_embeddings

logger = logging.getLogger(__name__)


def summary_path_info(out: BaseOutput) -> PathInfo:
    assert out.dvclive
    has_summary = True
    if isinstance(out.dvclive, dict):
        has_summary = out.dvclive.get(BaseOutput.PARAM_DVCLIVE_SUMMARY, True)
    if has_summary:
        return out.path_info.with_suffix(".json")
    return None


class DvcLive:
    def __init__(self, repo: Repo):
        self.repo = repo

    def summarize(self, path: str, revs: List[str] = None):
        if revs:
            revs = ["workspace", *revs]

        assert os.path.exists(path)

        parts = []

        metrics_path = path + ".json"
        with contextlib.suppress(NoMetricsError):
            metrics = self.repo.metrics.show(targets=[metrics_path])
            metrics_html = metrics_embedding(metrics)
            parts.extend([metrics_html, "<br>"])

        plots = self.repo.plots.show(targets=[path], recursive=True, revs=revs)
        embeddigns = plots_embeddings(plots)

        parts.extend(embeddigns)
        html = embed(parts)

        result_path = path + ".html"
        with open(result_path, "w") as fd:
            fd.write(html)
        logger.info(f"\nfile://{os.path.abspath(result_path)}")
