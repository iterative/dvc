import json
import logging
import os

from dvc.plot import DefaultTemplate
from dvc.repo import locked
from dvc.utils import format_link

logger = logging.getLogger(__name__)

PAGE_HTML = """<html>
<head>
    <title>dvc plot</title>
    <script src="https://cdn.jsdelivr.net/npm/vega@5.10.0"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@4.8.1"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6.5.1"></script>
</head>
<body>
    {divs}
</body>
</html>"""

DIV_HTML = """<div id = "{id}"></div>
<script type = "text/javascript">
    var spec = {vega_json};
    vegaEmbed('#{id}', spec);
</script>"""


def _save_plot_html(divs, path):
    page = PAGE_HTML.format(divs="\n".join(divs))
    with open(path, "w") as fobj:
        fobj.write(page)


def _prepare_div(vega_dict):
    from shortuuid import uuid

    id = uuid()
    return DIV_HTML.format(
        id=str(id),
        vega_json=json.dumps(vega_dict, indent=4, separators=(",", ": ")),
    )


def _load(tree, target, revision="current workspace"):
    with tree.open(target, "r") as fobj:
        data = json.load(fobj)
        for d in data:
            d["revision"] = revision
    return data


@locked
def plot(repo, targets, plot_path=None, typ="json"):

    if not plot_path:
        plot_path = "plot.html"

    divs = []
    for target in targets:
        data = _load(repo.tree, target)
        vega_plot_json = DefaultTemplate(repo.dvc_dir).fill(data, target)
        divs.append(_prepare_div(vega_plot_json))

    _save_plot_html(divs, plot_path)

    logger.info(
        "Your can see your plot by opening {} in your "
        "browser!".format(
            format_link(
                "file://{}".format(os.path.join(repo.root_dir, plot_path))
            )
        )
    )
