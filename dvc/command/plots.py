import argparse
import logging
import os

from dvc.command.base import CmdBase, append_doc_link, fix_subparsers
from dvc.exceptions import DvcException
from dvc.repo.plots.data import WORKSPACE_REVISION_NAME

logger = logging.getLogger(__name__)


class CmdPlots(CmdBase):
    def _revisions(self):
        raise NotImplementedError

    def _result_file(self):
        if self.args.file:
            return self.args.file

        extension = self._result_extension()
        base = self._result_basename()

        result_file = base + extension
        return result_file

    def _result_basename(self):
        if self.args.datafile:
            return self.args.datafile
        return "plot"

    def _result_extension(self):
        if not self.args.no_html:
            return ".html"
        elif self.args.template:
            return os.path.splitext(self.args.template)[-1]
        return ".json"

    def run(self):
        fields = None
        jsonpath = None
        if self.args.select:
            if self.args.select.startswith("$"):
                jsonpath = self.args.select
            else:
                fields = set(self.args.select.split(","))
        try:
            plot_string = self.repo.plot(
                datafile=self.args.datafile,
                template=self.args.template,
                revisions=self._revisions(),
                fields=fields,
                x_field=self.args.x,
                y_field=self.args.y,
                path=jsonpath,
                embed=not self.args.no_html,
                csv_header=not self.args.no_csv_header,
                title=self.args.title,
                x_title=self.args.xlab,
                y_title=self.args.ylab,
            )

            if self.args.stdout:
                logger.info(plot_string)
            else:
                result_path = self._result_file()
                with open(result_path, "w") as fobj:
                    fobj.write(plot_string)

                logger.info(
                    "file://{}".format(
                        os.path.join(self.repo.root_dir, result_path)
                    )
                )

        except DvcException:
            logger.exception("")
            return 1

        return 0


class CmdPlotsShow(CmdPlots):
    def _revisions(self):
        return None


class CmdPlotsDiff(CmdPlots):
    def _revisions(self):
        revisions = self.args.revisions or []
        if len(revisions) <= 1:
            if len(revisions) == 0 and self.repo.scm.is_dirty():
                revisions.append("HEAD")
            revisions.append(WORKSPACE_REVISION_NAME)
        return revisions


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
        "-f", "--file", default=None, help="Name of the generated file."
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
        "--stdout",
        action="store_true",
        default=False,
        help="Print plots specification to stdout.",
    )
    plots_show_parser.add_argument(
        "--no-csv-header",
        action="store_true",
        default=False,
        help="Required when CSV or TSV datafile does not have a header.",
    )
    plots_show_parser.add_argument(
        "--no-html",
        action="store_true",
        default=False,
        help="Do not wrap Vega plot JSON with HTML.",
    )
    plots_show_parser.add_argument("--title", default=None, help="Plot title.")
    plots_show_parser.add_argument(
        "--xlab", default=None, help="X axis title."
    )
    plots_show_parser.add_argument(
        "--ylab", default=None, help="Y axis title."
    )
    plots_show_parser.add_argument(
        "datafile", nargs="?", default=None, help="Metrics file to visualize",
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
        "-d",
        "--datafile",
        nargs="?",
        default=None,
        help="Metrics file to visualize",
    )
    plots_diff_parser.add_argument(
        "-f", "--file", default=None, help="Name of the generated file."
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
        "--stdout",
        action="store_true",
        default=False,
        help="Print plot specification to stdout.",
    )
    plots_diff_parser.add_argument(
        "--no-csv-header",
        action="store_true",
        default=False,
        help="Provided CSV ot TSV datafile does not have a header.",
    )
    plots_diff_parser.add_argument(
        "--no-html",
        action="store_true",
        default=False,
        help="Do not wrap Vega plot JSON with HTML.",
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
