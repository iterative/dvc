import logging
import os
from typing import List

from dvc.repo import Repo
from dvc.visualization import embed, metrics_embedding, plots_embeddings

logger = logging.getLogger(__name__)


class DvcLive:
    def __init__(self, repo: Repo):
        self.repo = repo

    def summarize(self, path: str, revs: List[str] = None):
        if revs:
            revs = ["workspace", *revs]

        assert os.path.exists(path)

        parts = []

        metrics_path = path + ".json"
        if os.path.exists(metrics_path):
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
        logger.info(f"file://{os.path.abspath(result_path)}")
