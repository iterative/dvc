import logging
import os

from funcy import first, last

from dvc.exceptions import DvcException
from dvc.repo.plot.data import PlotData
from dvc.repo.plot.template import Template, NoDataForTemplateError
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
    x_field=None,
    y_field=None,
    path=None,
    csv_header=True,
):

    template_datafiles, x_anchor, y_anchor = _parse_template(
        template_path, datafile
    )
    append_index = x_anchor and not x_field
    if append_index:
        x_field = PlotData.INDEX_FIELD

    template_data = {}
    for datafile in template_datafiles:
        from dvc.repo.plot.data import _load_from_revisions

        plot_datas = _load_from_revisions(repo, datafile, revisions)
        tmp_data = []
        for pd in plot_datas:
            rev_data_points = pd.to_datapoints(
                fields=fields,
                path=path,
                csv_header=csv_header,
                append_index=append_index,
            )

            if y_anchor and not y_field:
                all_fields = list(first(rev_data_points).keys())
                all_fields.remove(PlotData.REVISION_FIELD)
                if x_field and x_field in all_fields:
                    all_fields.remove(x_field)
                y_field = last(all_fields)
            tmp_data.extend(rev_data_points)

        template_data[datafile] = tmp_data

    if len(template_data) == 0:
        raise NoDataForTemplateError(template_path)

    return Template.fill(
        template_path, template_data, datafile, x_field, y_field
    )


def plot(
    repo,
    datafile=None,
    template=None,
    revisions=None,
    fields=None,
    x_field=None,
    y_field=None,
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
        repo,
        datafile,
        template_path,
        revisions,
        fields,
        x_field,
        y_field,
        path,
        csv_header,
    )

    if embed:
        div = DIV_HTML.format(id="plot", vega_json=plot_content)
        plot_content = PAGE_HTML.format(divs=div)

    return plot_content


def _parse_template(template_path, priority_datafile):
    with open(template_path, "r") as fobj:
        tempalte_content = fobj.read()

    template_datafiles = Template.parse_data_placeholders(tempalte_content)
    if priority_datafile:
        if len(template_datafiles) > 1:
            raise TooManyDataSourcesError(
                priority_datafile, template_datafiles
            )
        template_datafiles = {priority_datafile}

    return (
        template_datafiles,
        Template.X_AXIS_STRING in tempalte_content,
        Template.Y_AXIS_STRING in tempalte_content,
    )
