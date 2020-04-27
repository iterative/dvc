import json
import logging
import os

from funcy import first, last

from dvc.exceptions import DvcException
from dvc.repo.plot.data import PlotData
from dvc.template import Template, NoDataForTemplateError
from dvc.repo import locked

logger = logging.getLogger(__name__)

PAGE_HTML = """<html>
<head>
    <title>DVC plot</title>
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
    repo,
    datafile,
    template_path,
    revisions,
    fields=None,
    path=None,
    csv_header=True,
):
    default_plot = template_path == repo.plot_templates.default_template

    template_datafiles = _parse_template(template_path, datafile)

    template_data = {}
    for datafile in template_datafiles:
        from dvc.repo.plot.data import _load_from_revisions

        plot_datas = _load_from_revisions(repo, datafile, revisions)

        tmp_data = []
        for pd in plot_datas:
            rev_data_points = pd.to_datapoints(
                fields=fields, path=path, csv_header=csv_header
            )
            if default_plot:
                rev_data_points = _to_default_data(rev_data_points)
            tmp_data.extend(rev_data_points)

        template_data[datafile] = tmp_data

    if len(template_data) == 0:
        raise NoDataForTemplateError(template_path)

    filled_template = Template.fill(template_path, template_data, datafile)

    if default_plot:
        return _fix_default_template(template_data, filled_template)

    return filled_template


def _to_default_data(data_points):
    keys = list(first(data_points).keys())
    keys.remove(PlotData.REVISION_FIELD)
    data_field = last(keys)
    new_data = []
    for index, data_point in enumerate(data_points):
        new_data.append(
            {
                "x": index,
                data_field: data_point[data_field],
                PlotData.REVISION_FIELD: data_point[PlotData.REVISION_FIELD],
            }
        )
    return new_data


def _fix_default_template(template_data, plot_json):
    assert len(template_data) == 1
    datafile, data = first(template_data.items())

    keys = list(first(data).keys())
    keys.remove(PlotData.REVISION_FIELD)
    keys.remove("x")
    data_field = first(keys)

    tmp_plot = json.loads(plot_json)
    tmp_plot["title"] = datafile
    tmp_plot["encoding"]["y"]["field"] = data_field
    return json.dumps(
        tmp_plot, indent=4, separators=(",", ": "), sort_keys=True
    )


def plot(
    repo,
    datafile=None,
    template=None,
    revisions=None,
    fields=None,
    path=None,
    embed=False,
    csv_header=True,
):
    if revisions is None:
        from dvc.repo.plot.data import WORKSPACE_REVISION_NAME

        revisions = [WORKSPACE_REVISION_NAME]

    if not datafile and not template:
        raise NoDataOrTemplateProvided()

    template_path = _evaluate_templatepath(repo, template)

    plot_content = fill_template(
        repo, datafile, template_path, revisions, fields, path, csv_header
    )

    if embed:
        div = DIV_HTML.format(id="plot", vega_json=plot_content)
        plot_content = PAGE_HTML.format(divs=div)

    return plot_content


def _parse_template(template_path, datafile):
    template_datafiles = Template.parse_data_placeholders(template_path)
    if datafile:
        if len(template_datafiles) > 1:
            raise TooManyDataSourcesError(datafile, template_datafiles)
        template_datafiles = {datafile}
    return template_datafiles
