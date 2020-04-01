import json
import logging

from dvc.plot import DefaultTemplate

logger = logging.getLogger(__name__)


class PageTemplate:
    HTML = """<html>
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

    @staticmethod
    def save(divs, path):
        page = PageTemplate.HTML.format(divs="\n".join(divs))
        with open(path, "w") as fd:
            fd.write(page)


class DivTemplate:
    HTML = """
        <div id = "{id}"></div>
        <script type = "text/javascript">
          var spec = {vega_json};
          vegaEmbed('#{id}', spec);
        </script>"""

    @staticmethod
    def prepare_div(vega_dict):
        from shortuuid import uuid

        id = uuid()
        return DivTemplate.HTML.format(
            id=str(id),
            vega_json=json.dumps(vega_dict, indent=4, separators=(",", ": ")),
        )


def _load(tree, target, revision="current workspace"):
    with tree.open(target, "r") as fd:
        data = json.load(fd)
        for d in data:
            d["revision"] = revision
    return data


def plot(repo, targets, plot_filename="plot.html", typ="json"):
    divs = []
    for target in targets:
        data = _load(repo.tree, target)
        vega_plot_json = DefaultTemplate(repo.dvc_dir).fill(data)
        divs.append(DivTemplate.prepare_div(vega_plot_json))
    PageTemplate.save(divs, plot_filename)
