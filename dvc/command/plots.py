import argparse
import logging
import os

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link, fix_subparsers
from dvc.exceptions import DvcException
from dvc.schema import PLOT_PROPS
from dvc.utils import format_link

logger = logging.getLogger(__name__)

PAGE_HTML = """<!DOCTYPE html>
<html>
<head>
    <title>DVC Plot</title>
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


class CmdPlots(CmdBase):
    def _func(self, *args, **kwargs):
        raise NotImplementedError

    def _props(self):
        # Pass only props specified by user, to not shadow ones from plot def
        props = {p: getattr(self.args, p) for p in PLOT_PROPS}
        return {k: v for k, v in props.items() if v is not None}

    def run(self):
        if self.args.show_vega:
            if not self.args.targets:
                logger.error("please specify a target for `--show-vega`")
                return 1
            if len(self.args.targets) > 1:
                logger.error(
                    "you can only specify one target for `--show-vega`"
                )
                return 1

        try:
            plots = self._func(targets=self.args.targets, props=self._props())

            if self.args.show_vega:
                target = self.args.targets[0]
                logger.info(plots[target])
                return 0

            divs = [
                DIV_HTML.format(id=f"plot{i}", vega_json=plot)
                for i, plot in enumerate(plots.values())
            ]
            html = PAGE_HTML.format(divs="\n".join(divs))
            path = self.args.out or "plots.html"

            path = os.path.join(os.getcwd(), path)
            with open(path, "w") as fobj:
                fobj.write(html)

            logger.info(f"file://{path}")

        except DvcException:
            logger.exception("")
            return 1

        return 0


class CmdPlotsShow(CmdPlots):
    UNINITIALIZED = True

    def _func(self, *args, **kwargs):
        return self.repo.plots.show(*args, **kwargs)


class CmdPlotsDiff(CmdPlots):
    UNINITIALIZED = True

    def _func(self, *args, **kwargs):
        return self.repo.plots.diff(
            *args,
            revs=self.args.revisions,
            experiment=self.args.experiment,
            **kwargs,
        )


class CmdPlotsModify(CmdPlots):
    def run(self):
        self.repo.plots.modify(
            self.args.target, props=self._props(), unset=self.args.unset,
        )
        return 0


def add_parser(subparsers, parent_parser):
    PLOTS_HELP = (
        "Commands to visualize and compare plot metrics in structured files "
        "(JSON, YAML, CSV, TSV)"
    )

    plots_parser = subparsers.add_parser(
        "plots",
        parents=[parent_parser],
        description=append_doc_link(PLOTS_HELP, "plots"),
        help=PLOTS_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plots_subparsers = plots_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc plots CMD --help` to display command-specific help.",
    )

    fix_subparsers(plots_subparsers)

    SHOW_HELP = "Generate plots from metrics files."
    plots_show_parser = plots_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        description=append_doc_link(SHOW_HELP, "plots/show"),
        help=SHOW_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plots_show_parser.add_argument(
        "targets",
        nargs="*",
        help="Files to visualize (supports any file, "
        "even when not found as `plots` in `dvc.yaml`). "
        "Shows all plots by default.",
    ).complete = completion.FILE
    _add_props_arguments(plots_show_parser)
    _add_output_arguments(plots_show_parser)
    plots_show_parser.set_defaults(func=CmdPlotsShow)

    PLOTS_DIFF_HELP = (
        "Show multiple versions of plot metrics "
        "by plotting them in a single image."
    )
    plots_diff_parser = plots_subparsers.add_parser(
        "diff",
        parents=[parent_parser],
        description=append_doc_link(PLOTS_DIFF_HELP, "plots/diff"),
        help=PLOTS_DIFF_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plots_diff_parser.add_argument(
        "--targets",
        nargs="*",
        help="Files to visualize (supports any file, "
        "even when not found as `plots` in `dvc.yaml`). "
        "Shows all plots by default.",
        metavar="<path>",
    ).complete = completion.FILE
    plots_diff_parser.add_argument(
        "-e",
        "--experiment",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,
    )
    plots_diff_parser.add_argument(
        "revisions", nargs="*", default=None, help="Git commits to plot from",
    )
    _add_props_arguments(plots_diff_parser)
    _add_output_arguments(plots_diff_parser)
    plots_diff_parser.set_defaults(func=CmdPlotsDiff)

    PLOTS_MODIFY_HELP = "Modify display properties of plot metrics files."
    plots_modify_parser = plots_subparsers.add_parser(
        "modify",
        parents=[parent_parser],
        description=append_doc_link(PLOTS_MODIFY_HELP, "plots/modify"),
        help=PLOTS_MODIFY_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plots_modify_parser.add_argument(
        "target", help="Metric file to set properties to",
    ).complete = completion.FILE
    _add_props_arguments(plots_modify_parser)
    plots_modify_parser.add_argument(
        "--unset",
        nargs="*",
        metavar="<property>",
        help="Unset one or more display properties.",
    )
    plots_modify_parser.set_defaults(func=CmdPlotsModify)


def _add_props_arguments(parser):
    parser.add_argument(
        "-t",
        "--template",
        nargs="?",
        default=None,
        help=(
            "Special JSON or HTML schema file to inject with the data. "
            "See {}".format(
                format_link("https://man.dvc.org/plots#plot-templates")
            )
        ),
        metavar="<path>",
    ).complete = completion.FILE
    parser.add_argument(
        "-x", default=None, help="Field name for X axis.", metavar="<field>"
    )
    parser.add_argument(
        "-y", default=None, help="Field name for Y axis.", metavar="<field>"
    )
    parser.add_argument(
        "--no-header",
        action="store_false",
        dest="header",
        default=None,  # Use default None to distinguish when it's not used
        help="Provided CSV or TSV datafile does not have a header.",
    )
    parser.add_argument(
        "--title", default=None, metavar="<text>", help="Plot title."
    )
    parser.add_argument(
        "--x-label", default=None, help="X axis label", metavar="<text>"
    )
    parser.add_argument(
        "--y-label", default=None, help="Y axis label", metavar="<text>"
    )


def _add_output_arguments(parser):
    parser.add_argument(
        "-o",
        "--out",
        default=None,
        help="Destination path to save plots to",
        metavar="<path>",
    ).complete = completion.DIR
    parser.add_argument(
        "--show-vega",
        action="store_true",
        default=False,
        help="Show output in Vega format.",
    )
