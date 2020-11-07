import logging

from funcy import cached_property, first, project

from dvc.exceptions import (
    DvcException,
    NoMetricsFoundError,
    NoMetricsParsedError,
)
from dvc.repo.collect import collect
from dvc.repo.plots.data import PlotParsingError
from dvc.schema import PLOT_PROPS
from dvc.tree.repo import RepoTree
from dvc.utils import relpath

logger = logging.getLogger(__name__)


class NotAPlotError(DvcException):
    def __init__(self, out):
        super().__init__(
            f"'{out}' is not a known plot. Use `dvc plots modify` to turn it "
            "into one."
        )


class PropsNotFoundError(DvcException):
    pass


class Plots:
    def __init__(self, repo):
        self.repo = repo

    def collect(self, targets=None, revs=None):
        """Collects all props and data for plots.

        Returns a structure like:
            {rev: {plots.csv: {
                props: {x: ..., "header": ..., ...},
                data: "...data as a string...",
            }}}
        Data parsing is postponed, since it's affected by props.
        """
        targets = [targets] if isinstance(targets, str) else targets or []
        data = {}
        for rev in self.repo.brancher(revs=revs):
            # .brancher() adds unwanted workspace
            if revs is not None and rev not in revs:
                continue
            rev = rev or "workspace"

            tree = RepoTree(self.repo)
            plots = _collect_plots(self.repo, targets, rev)
            for path_info, props in plots.items():
                datafile = relpath(path_info, self.repo.root_dir)
                if rev not in data:
                    data[rev] = {}
                data[rev].update({datafile: {"props": props}})

                # Load data from git or dvc cache
                try:
                    with tree.open(path_info) as fd:
                        data[rev][datafile]["data"] = fd.read()
                except FileNotFoundError:
                    # This might happen simply because cache is absent
                    pass

        return data

    @staticmethod
    def render(data, revs=None, props=None, templates=None):
        """Renders plots"""
        props = props or {}

        # Merge data by plot file and apply overriding props
        plots = _prepare_plots(data, revs, props)

        result = {}
        for datafile, desc in plots.items():
            try:
                result[datafile] = _render(
                    datafile, desc["data"], desc["props"], templates
                )
            except PlotParsingError as e:
                logger.debug(
                    "failed to read '%s' on '%s'",
                    e.path,
                    e.revision,
                    exc_info=True,
                )

        if not any(result.values()):
            raise NoMetricsParsedError("plots")

        return result

    def show(self, targets=None, revs=None, props=None, templates=None):
        from .data import NoMetricInHistoryError

        data = self.collect(targets, revs)

        # If any mentioned plot doesn't have any data then that's an error
        targets = [targets] if isinstance(targets, str) else targets or []
        for target in targets:
            rpath = relpath(target, self.repo.root_dir)
            if not any("data" in d[rpath] for d in data.values()):
                raise NoMetricInHistoryError(target)

        # No data at all is a special error with a special message
        if not data:
            raise NoMetricsFoundError("plots", "--plots/--plots-no-cache")

        if templates is None:
            templates = self.templates
        return self.render(data, revs, props, templates)

    def diff(self, *args, **kwargs):
        from .diff import diff

        return diff(self.repo, *args, **kwargs)

    @staticmethod
    def _unset(out, props):
        missing = list(set(props) - set(out.plot.keys()))
        if missing:
            raise PropsNotFoundError(
                f"display properties {missing} not found in plot '{out}'"
            )

        for prop in props:
            out.plot.pop(prop)

    def modify(self, path, props=None, unset=None):
        from dvc.dvcfile import Dvcfile

        props = props or {}
        template = props.get("template")
        if template:
            self.templates.get_template(template)

        (out,) = self.repo.find_outs_by_path(path)
        if not out.plot and unset is not None:
            raise NotAPlotError(out)

        # This out will become a plot unless it is one already
        if not isinstance(out.plot, dict):
            out.plot = {}

        if unset:
            self._unset(out, unset)

        out.plot.update(props)

        # Empty dict will move it to non-plots
        if not out.plot:
            out.plot = True

        out.verify_metric()

        dvcfile = Dvcfile(self.repo, out.stage.path)
        dvcfile.dump(out.stage, update_lock=False)

    @cached_property
    def templates(self):
        from .template import PlotTemplates

        return PlotTemplates(self.repo.dvc_dir)


def _is_plot(out):
    return bool(out.plot)


def _collect_plots(repo, targets=None, rev=None):
    plots, path_infos = collect(
        repo, output_filter=_is_plot, targets=targets, rev=rev
    )
    result = {plot.path_info: _plot_props(plot) for plot in plots}
    result.update({path_info: {} for path_info in path_infos})
    return result


def _plot_props(out):
    if not out.plot:
        raise NotAPlotError(out)
    if isinstance(out.plot, list):
        raise DvcException("Multiple plots per data file not supported.")
    if isinstance(out.plot, bool):
        return {}

    return project(out.plot, PLOT_PROPS)


def _prepare_plots(data, revs, props):
    """Groups data by plot file.

    Also resolves props conflicts between revs and applies global props.
    """
    # we go in order revs are supplied on props conflict first ones win.
    revs = iter(data) if revs is None else revs

    plots, props_revs = {}, {}
    for rev in revs:
        # Asked for revision without data
        if rev not in data:
            continue

        for datafile, desc in data[rev].items():
            # We silently skip on an absent data file,
            # see also try/except/pass in .collect()
            if "data" not in desc:
                continue

            # props from command line overwrite plot props from out definition
            full_props = {**desc["props"], **props}

            if datafile in plots:
                saved = plots[datafile]
                if saved["props"] != full_props:
                    logger.warning(
                        f"Inconsistent plot props for '{datafile}' in "
                        f"'{props_revs[datafile]}' and '{rev}'. "
                        f"Going to use ones from '{props_revs[datafile]}'"
                    )

                saved["data"][rev] = desc["data"]
            else:
                plots[datafile] = {
                    "props": full_props,
                    "data": {rev: desc["data"]},
                }
                # Save rev we got props from
                props_revs[datafile] = rev

    return plots


def _render(datafile, datas, props, templates):
    from .data import PlotData, plot_data

    # Copy it to not modify a passed value
    props = props.copy()

    # Add x and y to fields if set
    fields = props.get("fields")
    if fields is not None:
        fields = {*fields, props.get("x"), props.get("y")} - {None}

    template = templates.load(props.get("template") or "default")

    # If x is not set add index field
    if not props.get("x") and template.has_anchor("x"):
        props["append_index"] = True
        props["x"] = PlotData.INDEX_FIELD

    # Parse all data, preprocess it and collect as a list of dicts
    data = []
    for rev, datablob in datas.items():
        rev_data = plot_data(datafile, rev, datablob).to_datapoints(
            fields=fields,
            path=props.get("path"),
            header=props.get("header", True),
            append_index=props.get("append_index", False),
        )
        data.extend(rev_data)

    # If y is not set then use last field not used yet
    if not props.get("y") and template.has_anchor("y"):
        fields = list(first(data))
        skip = (PlotData.REVISION_FIELD, props.get("x"))
        props["y"] = first(f for f in reversed(fields) if f not in skip)

    return template.render(data, props=props)
