import json
import logging
import random
import re
import string

from dvc.exceptions import DvcException
from dvc.plot import Template
from dvc.repo import locked

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
    id = "".join(random.sample(string.ascii_lowercase, 8))
    return DIV_HTML.format(
        id=str(id),
        vega_json=json.dumps(vega_dict, indent=4, separators=(",", ": ")),
    )


def _load_data(tree, target, revision="current workspace"):
    with tree.open(target, "r") as fobj:
        data = json.load(fobj)
        for d in data:
            d["revision"] = revision
    return data


def _load_from_rev(repo, revisions, target):
    data = []
    if len(revisions) == 0:
        if repo.scm.is_dirty():
            data.extend(_load_data(repo.scm.get_tree("HEAD"), target, "HEAD"))
        data.extend(_load_data(repo.tree, target))
        logger.error(data)
    elif len(revisions) == 1:
        data.extend(
            _load_data(repo.scm.get_tree(revisions[0]), target, revisions[0])
        )
        data.extend(_load_data(repo.tree, target))
    else:
        for rev in revisions:
            data.extend(_load_data(repo.scm.get_tree(rev), target, rev))
    return data


def _parse_plots(path):
    with open(path, "r") as fobj:
        content = fobj.read()

    plot_regex = re.compile("<DVC_PLOT::.*>")

    plots = list(plot_regex.findall(content))
    return False, plots


def _parse_plot_str(plot_str):
    content = plot_str.replace("<", "")
    content = content.replace(">", "")
    args = content.split("::")[1:]
    if len(args) == 2:
        return args
    elif len(args) == 1:
        return args[0], "default.json"
    raise DvcException("Error parsing")


def to_div(repo, plot_str):
    datafile, templatefile = _parse_plot_str(plot_str)

    data = _load_data(repo.tree, datafile)
    vega_plot_json = Template(repo.plot_templates.templates_dir).fill(
        templatefile, data, datafile
    )
    return _prepare_div(vega_plot_json)


@locked
def plot(repo, template_file, revisions=None):
    if revisions is None:
        revisions = []

    is_html, plot_strings = _parse_plots(template_file)
    m = {plot_str: to_div(repo, plot_str) for plot_str in plot_strings}

    result = template_file.replace(".dvct", ".html")
    if not is_html:
        _save_plot_html(
            [m[p] for p in plot_strings], result,
        )
        return result
    else:
        raise NotImplementedError
