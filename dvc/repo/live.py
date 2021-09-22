import logging
import os
from typing import TYPE_CHECKING, List, Optional

from dvc.render.utils import render

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dvc.output import Output
    from dvc.path_info import PathInfo
    from dvc.repo import Repo


LIVE_IMAGES_PATH = "images"
LIVE_HTML_PATH = "html"
LIVE_SCALARS_PATH = "scalars"
LIVE_SUMMARY_PATH = "summary.json"


def create_live_html(out):
    assert out.live and out.live["html"]

    metrics, plots = out.repo.live.show(out.path_info)

    html_path = out.path_info / LIVE_HTML_PATH

    index_path = render(
        out.repo, plots, metrics=metrics, path=html_path, refresh_seconds=5
    )
    logger.info(f"\nfile://{os.path.abspath(index_path)}")


def summary_path_info(out: "Output") -> Optional["PathInfo"]:
    from dvc.output import Output

    assert out.live
    has_summary = True
    if isinstance(out.live, dict):
        has_summary = out.live.get(Output.PARAM_LIVE_SUMMARY, True)
    if has_summary:
        return out.path_info / LIVE_SUMMARY_PATH
    return None


class Live:
    def __init__(self, repo: "Repo"):
        self.repo = repo

    def show(self, target: str, revs: List[str] = None):
        if revs:
            revs = ["workspace", *revs]

        metrics_path = os.path.join(target, LIVE_SUMMARY_PATH)
        images_path = os.path.join(target, LIVE_IMAGES_PATH)
        scalars_path = os.path.join(target, LIVE_SCALARS_PATH)
        
        metrics = self.repo.metrics.show(targets=[metrics_path])

        plots = self.repo.plots.show(
            targets=[str(scalars_path), str(images_path)], 
            recursive=True, revs=revs)

        return metrics, plots
