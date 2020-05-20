import logging
import os
from collections import defaultdict

from funcy import first, last

from dvc.exceptions import DvcException
from dvc.repo import locked

from .data import PlotData
from .template import NoDataForTemplateError, Template

logger = logging.getLogger(__name__)


class TooManyDataSourcesError(DvcException):
    def __init__(self, datafile, template_datafiles):
        super().__init__(
            "Unable to infer which of possible data sources: '{}' "
            "should be replaced with '{}'.".format(
                ", ".join(template_datafiles), datafile
            )
        )


class NoDataOrTemplateProvided(DvcException):
    def __init__(self):
        super().__init__("Datafile or template is not specified.")


class TooManyTemplatesError(DvcException):
    pass


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
    x_field=None,
    y_field=None,
    **kwargs,
):
    if x_field and fields:
        fields.add(x_field)

    if y_field and fields:
        fields.add(y_field)

    template_datafiles, x_anchor, y_anchor = _parse_template(
        template_path, datafile
    )
    append_index = x_anchor and not x_field
    if append_index:
        x_field = PlotData.INDEX_FIELD

    template_data = {}
    for template_datafile in template_datafiles:
        from .data import _load_from_revisions

        plot_datas = _load_from_revisions(repo, template_datafile, revisions)
        tmp_data = []
        for pd in plot_datas:
            rev_data_points = pd.to_datapoints(
                fields=fields,
                path=path,
                csv_header=csv_header,
                append_index=append_index,
            )

            if y_anchor and not y_field:
                y_field = _infer_y_field(rev_data_points, x_field)
            tmp_data.extend(rev_data_points)

        template_data[template_datafile] = tmp_data

    if len(template_data) == 0:
        raise NoDataForTemplateError(template_path)

    content = Template.fill(
        template_path,
        template_data,
        priority_datafile=datafile,
        x_field=x_field,
        y_field=y_field,
        **kwargs,
    )

    path = datafile or ",".join(template_datafiles)

    return path, content


def _infer_y_field(rev_data_points, x_field):
    all_fields = list(first(rev_data_points).keys())
    all_fields.remove(PlotData.REVISION_FIELD)
    if x_field and x_field in all_fields:
        all_fields.remove(x_field)
    y_field = last(all_fields)
    return y_field


def _show(repo, datafile=None, template=None, revs=None, **kwargs):
    if revs is None:
        revs = ["working tree"]

    if not datafile and not template:
        raise NoDataOrTemplateProvided()

    template_path = _evaluate_templatepath(repo, template)

    plot_datafile, plot_content = fill_template(
        repo, datafile, template_path, revs, **kwargs
    )

    return plot_datafile, plot_content


def _parse_template(template_path, priority_datafile):
    with open(template_path) as fobj:
        tempalte_content = fobj.read()

    template_datafiles = Template.parse_data_anchors(tempalte_content)
    if priority_datafile:
        if len(template_datafiles) > 1:
            raise TooManyDataSourcesError(
                priority_datafile, template_datafiles
            )
        template_datafiles = {priority_datafile}

    return (
        template_datafiles,
        Template.X_ANCHOR in tempalte_content,
        Template.Y_ANCHOR in tempalte_content,
    )


def _collect_plots(repo):
    plots = defaultdict(set)

    for stage in repo.stages:
        for out in stage.outs:
            if not out.plot:
                continue

            if isinstance(out.plot, dict):
                template = out.plot[out.PARAM_PLOT_TEMPLATE]
            else:
                template = None

            plots[str(out)].add(template)

    return plots


def show(repo, targets=None, template=None, revs=None, **kwargs) -> dict:
    if isinstance(targets, str):
        targets = [targets]

    if targets:
        for target in targets:
            return {
                target: _show(
                    repo,
                    datafile=target,
                    template=template,
                    revs=revs,
                    **kwargs,
                )[1]
            }

    if not revs:
        plots = _collect_plots(repo)
    else:
        plots = defaultdict(set)
        for rev in repo.brancher(revs=revs):
            for plot, templates in _collect_plots(repo).items():
                plots[plot].update(templates)

    if not plots:
        datafile, plot = _show(
            repo, datafile=None, template=template, revs=revs, **kwargs
        )
        return {datafile: plot}

    ret = {}
    for plot, templates in plots.items():
        tmplt = template
        if len(templates) == 1:
            tmplt = list(templates)[0]
        elif not template:
            raise TooManyTemplatesError(
                f"'{plot}' uses multiple templates '{templates}'. "
                "Use `-t|--template` to specify the template to use. "
            )

        ret[plot] = _show(
            repo, datafile=plot, template=tmplt, revs=revs, **kwargs
        )[1]

    return ret
