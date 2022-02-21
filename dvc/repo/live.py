import logging
import os
from typing import TYPE_CHECKING, List, Optional

from dvc_render import render_html
from funcy import once_per_args

from dvc.render.match import match_renderers

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dvc.output import Output
    from dvc.repo import Repo


@once_per_args
def webbrowser_open(url: str) -> int:
    from dvc.ui import ui

    return ui.open_browser(url)


def create_summary(out):
    assert out.live and out.live["html"]

    metrics, plots = out.repo.live.show(out.fs_path)

    html_path = os.path.join(out.fs_path + "_dvc_plots", "index.html")

    renderers = match_renderers(
        plots, templates_dir=out.repo.plots.templates_dir
    )
    index_path = render_html(
        renderers, output_file=html_path, metrics=metrics, refresh_seconds=5
    )
    if out.repo.config["plots"].get("auto_open", False):
        webbrowser_open(index_path)


def summary_fs_path(out: "Output") -> Optional[str]:
    from dvc.output import Output

    assert out.live
    has_summary = True
    if isinstance(out.live, dict):
        has_summary = out.live.get(Output.PARAM_LIVE_SUMMARY, True)
    if has_summary:
        return out.fs.path.with_suffix(out.fs_path, ".json")
    return None


class Live:
    def __init__(self, repo: "Repo"):
        self.repo = repo

    def show(self, target: str, revs: List[str] = None):
        if revs:
            revs = ["workspace", *revs]

        metrics_path = target + ".json"

        metrics = self.repo.metrics.show(targets=[metrics_path])
        plots = self.repo.plots.show(target, recursive=True, revs=revs)

        return metrics, plots
