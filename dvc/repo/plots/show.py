import copy
import logging
import os

from funcy import first, last, project

from dvc.exceptions import DvcException, NoPlotsError
from dvc.repo import locked
from dvc.schema import PLOT_PROPS

from .data import NoMetricInHistoryError, PlotData
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
    repo, datafile, template_path, revisions, props,
):
    # Copy things to not modify passed values
    props = props.copy()
    fields = copy.copy(props.get("fields"))

    if props.get("x") and fields:
        fields.add(props.get("x"))

    if props.get("y") and fields:
        fields.add(props.get("y"))

    template_datafiles, x_anchor, y_anchor = _parse_template(
        template_path, datafile
    )
    append_index = x_anchor and not props.get("x")
    if append_index:
        props["x"] = PlotData.INDEX_FIELD

    template_data = {}
    for template_datafile in template_datafiles:
        from .data import _load_from_revisions

        plot_datas = _load_from_revisions(repo, template_datafile, revisions)
        tmp_data = []
        for pd in plot_datas:
            rev_data_points = pd.to_datapoints(
                fields=fields,
                path=props.get("path"),
                csv_header=props.get("csv_header", True),
                append_index=append_index,
            )

            if y_anchor and not props.get("y"):
                props["y"] = _infer_y_field(rev_data_points, props.get("x"))
            tmp_data.extend(rev_data_points)

        template_data[template_datafile] = tmp_data

    if len(template_data) == 0:
        raise NoDataForTemplateError(template_path)

    content = Template.fill(
        template_path, template_data, priority_datafile=datafile, props=props,
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


def _show(repo, datafile=None, revs=None, props=None):
    if revs is None:
        revs = ["workspace"]

    if props is None:
        props = {}

    if not datafile and not props.get("template"):
        raise NoDataOrTemplateProvided()

    template_path = _evaluate_templatepath(repo, props.get("template"))

    plot_datafile, plot_content = fill_template(
        repo, datafile, template_path, revs, props
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


def _collect_plots(repo, targets=None):
    from dvc.exceptions import OutputNotFoundError
    from contextlib import suppress

    def _targets_to_outs(targets):
        for t in targets:
            with suppress(OutputNotFoundError):
                (out,) = repo.find_outs_by_path(t)
                yield out

    if targets:
        outs = _targets_to_outs(targets)
    else:
        outs = (out for stage in repo.stages for out in stage.outs if out.plot)

    return {str(out): _plot_props(out) for out in outs}


def _plot_props(out):
    if not out.plot:
        raise DvcException(
            f"'{out}' is not a plot. Use `dvc plots modify` to change that."
        )
    if isinstance(out.plot, list):
        raise DvcException("Multiple plots per data file not supported yet.")
    if isinstance(out.plot, bool):
        return {}

    return project(out.plot, PLOT_PROPS)


def show(repo, targets=None, revs=None, props=None) -> dict:
    if isinstance(targets, str):
        targets = [targets]
    if props is None:
        props = {}

    # Collect plot data files with associated props
    plots = {}
    for rev in repo.brancher(revs=revs):
        if revs is not None and rev not in revs:
            continue

        for datafile, file_props in _collect_plots(repo, targets).items():
            # props from command line overwrite plot props from out definition
            full_props = {**file_props, **props}

            if datafile in plots:
                saved_rev, saved_props = plots[datafile]
                if saved_props != props:
                    logger.warning(
                        f"Inconsistent plot props for '{datafile}' in "
                        f"'{saved_rev}' and '{rev}'. "
                        f"Going to use ones from '{saved_rev}'"
                    )
            else:
                plots[datafile] = rev, full_props

    if not plots:
        if targets:
            raise NoMetricInHistoryError(", ".join(targets))

        try:
            datafile, plot = _show(repo, datafile=None, revs=revs, props=props)
        except NoDataOrTemplateProvided:
            raise NoPlotsError()

        return {datafile: plot}

    return {
        datafile: _show(repo, datafile=datafile, revs=revs, props=props)[1]
        for datafile, (_, props) in plots.items()
    }
