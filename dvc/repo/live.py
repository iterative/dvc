import logging
from pathlib import Path
from platform import uname
from typing import TYPE_CHECKING, List, Optional

from funcy import once_per_args

from dvc.render.utils import render

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dvc.output import Output
    from dvc.repo import Repo


@once_per_args
def webbrowser_open(url: str) -> int:
    import webbrowser

    return webbrowser.open(url)


def create_summary(out):

    assert out.live and out.live["html"]

    metrics, plots = out.repo.live.show(out.fs_path)

    html_path = out.fs_path + "_dvc_plots"

    index_path = render(
        out.repo, plots, metrics=metrics, path=html_path, refresh_seconds=5
    )

    url = index_path.as_uri()

    if "Microsoft" in uname().release:
        url = Path(index_path) / "index.html"

    webbrowser_open(url)


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
