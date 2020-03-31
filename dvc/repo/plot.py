import json
import logging

from dvc.plot import DefaultTemplate

logger = logging.getLogger(__name__)


class PageTemplate:
    HTML = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Embedding Vega-Lite</title>
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
          var spec = {vega_dict};
          vegaEmbed('#{id}', spec);
        </script>"""

    @staticmethod
    def prepare_div(vega_dict):
        from shortuuid import uuid

        id = uuid()
        return DivTemplate.HTML.format(
            id=str(id),
            vega_dict=json.dumps(vega_dict, indent=4, separators=(",", ": ")),
        )


def _load(tree, target, revision="curren workspace"):
    with tree.open(target, "r") as fd:
        data = json.load(fd)
        for d in data:
            d["revision"] = revision
    return data


def create_data_dict(repo, target, typ, a_rev=None, b_rev=None):
    result = {}
    data = []
    if typ == "json":

        if a_rev and b_rev:
            logger.error("diff")
            a_tree = repo.scm.get_tree(a_rev)
            b_tree = repo.scm.get_tree(b_rev)
            logger.error((a_tree, b_tree))
            data.extend(_load(a_tree, target, a_rev))
            data.extend(_load(b_tree, target, b_rev))
        else:
            logger.error(str(repo.tree.tree))
            data.extend(_load(repo.tree, target))

    result["data"] = {}
    result["data"]["values"] = data
    result["title"] = target
    return result


def plot(repo, targets, a_rev=None, b_rev=None, typ="json"):
    # TODO how to handle multiple targets
    logger.error((a_rev, b_rev))
    divs = []
    for target in targets:
        vega_data_dict = create_data_dict(repo, target, typ, a_rev, b_rev)

        # TODO need to pass title, probably need a way to pass additional conf

        vega_dict = DefaultTemplate(repo.dvc_dir).fill(vega_data_dict)
        divs.append(DivTemplate.prepare_div(vega_dict))
    PageTemplate.save(divs, "viz.html")
