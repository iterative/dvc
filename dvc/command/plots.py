import argparse
import logging
import os

from dvc.command.base import CmdBase, append_doc_link, fix_subparsers
from dvc.exceptions import DvcException

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

    def run(self):
        fields = None
        jsonpath = None
        if self.args.select:
            if self.args.select.startswith("$"):
                jsonpath = self.args.select
            else:
                fields = set(self.args.select.split(","))
        try:
            plots = self._func(
                targets=self.args.targets,
                template=self.args.template,
                fields=fields,
                x_field=self.args.x,
                y_field=self.args.y,
                path=jsonpath,
                csv_header=not self.args.no_csv_header,
                title=self.args.title,
                x_title=self.args.xlab,
                y_title=self.args.ylab,
            )

            if self.args.show_json:
                import json

                logger.info(json.dumps(plots))
                return 0

            divs = [
                DIV_HTML.format(id=f"plot{i}", vega_json=plot)
                for i, plot in enumerate(plots.values())
            ]
            html = PAGE_HTML.format(divs="\n".join(divs))
            path = self.args.out or "plots.html"

            with open(path, "w") as fobj:
                fobj.write(html)

            logger.info(
                "file://{}".format(os.path.join(self.repo.root_dir, path))
            )

        except DvcException:
            logger.exception("")
            return 1

        return 0


class CmdPlotsShow(CmdPlots):
    def _func(self, *args, **kwargs):
        return self.repo.plots.show(*args, **kwargs)


class CmdPlotsDiff(CmdPlots):
    def _func(self, *args, **kwargs):
        return self.repo.plots.diff(*args, revs=self.args.revisions, **kwargs)


def add_parser(subparsers, parent_parser):
    PLOTS_HELP = (
        "Generating plots for metrics stored in structured files "
        "(JSON, CSV, TSV)."
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

    SHOW_HELP = "Generate a plots image file from a metrics file."
    plots_show_parser = plots_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        description=append_doc_link(SHOW_HELP, "plots/show"),
        help=SHOW_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plots_show_parser.add_argument(
        "-t",
        "--template",
        nargs="?",
        default=None,
        help="File to be injected with data.",
    )
    plots_show_parser.add_argument(
        "-o", "--out", default=None, help="Destination path to save plots to.",
    )
    plots_show_parser.add_argument(
        "-s",
        "--select",
        default=None,
        help="Choose which field(s) or JSONPath to include in the plots.",
    )
    plots_show_parser.add_argument(
        "-x", default=None, help="Field name for x axis."
    )
    plots_show_parser.add_argument(
        "-y", default=None, help="Field name for y axis."
    )
    plots_show_parser.add_argument(
        "--no-csv-header",
        action="store_true",
        default=False,
        help="Required when CSV or TSV datafile does not have a header.",
    )
    plots_show_parser.add_argument(
        "--show-json",
        action="store_true",
        default=False,
        help="Show output in JSON format.",
    )
    plots_show_parser.add_argument("--title", default=None, help="Plot title.")
    plots_show_parser.add_argument(
        "--xlab", default=None, help="X axis title."
    )
    plots_show_parser.add_argument(
        "--ylab", default=None, help="Y axis title."
    )
    plots_show_parser.add_argument(
        "targets",
        nargs="*",
        help="Metrics files to visualize. Shows all plots by default.",
    )
    plots_show_parser.set_defaults(func=CmdPlotsShow)

    PLOTS_DIFF_HELP = (
        "Plot differences in metrics between commits in the DVC "
        "repository, or between the last commit and the workspace."
    )
    plots_diff_parser = plots_subparsers.add_parser(
        "diff",
        parents=[parent_parser],
        description=append_doc_link(PLOTS_DIFF_HELP, "plots/diff"),
        help=PLOTS_DIFF_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plots_diff_parser.add_argument(
        "-t",
        "--template",
        nargs="?",
        default=None,
        help="File to be injected with data.",
    )
    plots_diff_parser.add_argument(
        "--targets",
        nargs="*",
        help="Metrics file to visualize. Shows all plots by default.",
    )
    plots_diff_parser.add_argument(
        "-o", "--out", default=None, help="Destination path to save plots to.",
    )
    plots_diff_parser.add_argument(
        "-s",
        "--select",
        default=None,
        help="Choose which field(s) or JSONPath to include in the plot.",
    )
    plots_diff_parser.add_argument(
        "-x", default=None, help="Field name for x axis."
    )
    plots_diff_parser.add_argument(
        "-y", default=None, help="Field name for y axis."
    )
    plots_diff_parser.add_argument(
        "--no-csv-header",
        action="store_true",
        default=False,
        help="Provided CSV ot TSV datafile does not have a header.",
    )
    plots_diff_parser.add_argument(
        "--show-json",
        action="store_true",
        default=False,
        help="Show output in JSON format.",
    )
    plots_diff_parser.add_argument("--title", default=None, help="Plot title.")
    plots_diff_parser.add_argument(
        "--xlab", default=None, help="X axis title."
    )
    plots_diff_parser.add_argument(
        "--ylab", default=None, help="Y axis title."
    )
    plots_diff_parser.add_argument(
        "revisions", nargs="*", default=None, help="Git commits to plot from",
    )
    plots_diff_parser.set_defaults(func=CmdPlotsDiff)
