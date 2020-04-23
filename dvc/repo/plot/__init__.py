import itertools
import json
import logging
import os

from funcy import first

from dvc.exceptions import DvcException
from dvc.template import Template
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


class TooManyDataSourcesError(DvcException):
    def __init__(self, datafile, template_datafiles):
        super().__init__(
            "Unable to reason which of possible data sources: '{}' "
            "should be replaced with '{}'".format(
                ", ".join(template_datafiles), datafile
            )
        )


class NoDataOrTemplateProvided(DvcException):
    def __init__(self):
        super().__init__("Datafile or template is not specified.")


def _evaluate_templatepath(repo, template=None):
    if not template:
        return repo.plot_templates.default_template

    if os.path.exists(template):
        return template
    return repo.plot_templates.get_template(template)


@locked
def fill_template(
    repo, datafile, template_path, revisions, fields=None, path=None
):
    default_plot = template_path == repo.plot_templates.default_template

    template_datafiles = _parse_template(template_path, datafile)

    template_data = {}
    for datafile in template_datafiles:
        from dvc.repo.plot.data import _load_from_revisions

        plot_datas = _load_from_revisions(repo, datafile, revisions)
        template_data[datafile] = list(
            itertools.chain.from_iterable(
                [
                    pd.to_datapoints(
                        fields=fields, default_plot=default_plot, path=path
                    )
                    for pd in plot_datas
                ]
            )
        )

    filled_template = Template.fill(template_path, template_data, datafile)

    if default_plot:
        assert len(template_data) == 1
        tmp_plot = json.loads(filled_template)
        tmp_plot["title"] = first(template_data.keys())
        tmp_plot["encoding"]["y"]["field"] = first(
            set(first(first(template_data.values())).keys()) - {"x", "rev"}
        )
        filled_template = json.dumps(
            tmp_plot, indent=4, separators=(",", ": "), sort_keys=True
        )

    return filled_template


def plot(
    repo,
    datafile=None,
    template=None,
    revisions=None,
    fname=None,
    fields=None,
    path=None,
    embed=False,
):
    if revisions is None:
        from dvc.repo.plot.data import WORKSPACE_REVISION_NAME

        revisions = [WORKSPACE_REVISION_NAME]

    if not datafile and not template:
        raise NoDataOrTemplateProvided()

    template_path = _evaluate_templatepath(repo, template)

    plot_content = fill_template(
        repo, datafile, template_path, revisions, fields, path
    )

    if embed:
        div = DIV_HTML.format(id="plot", vega_json=plot_content)
        plot_content = PAGE_HTML.format(divs=div)

    if not fname:
        fname = _infer_result_file(datafile, template_path, embed)

    with open(fname, "w") as fobj:
        fobj.write(plot_content)
    return fname


def _infer_result_file(datafile, template_path, embed):
    if datafile:
        tmp = datafile
    else:
        tmp = "plot"

    if not embed:
        extension = os.path.splitext(template_path)[1]
    else:
        extension = ".html"

    result_file = os.path.splitext(tmp)[0] + extension

    if result_file == datafile or result_file == template_path:
        raise DvcException(
            "Could not infer plot name, please provide it " "with -f option."
        )
    return result_file


def _parse_template(template_path, datafile):
    template_datafiles = Template.parse_data_placeholders(template_path)
    if datafile:
        if len(template_datafiles) > 1:
            raise TooManyDataSourcesError(datafile, template_datafiles)
        template_datafiles = {datafile}
    return template_datafiles
