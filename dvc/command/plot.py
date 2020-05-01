import argparse
import logging
import os

from dvc.command.base import append_doc_link, CmdBase, fix_subparsers
from dvc.exceptions import DvcException
from dvc.repo.plot.data import WORKSPACE_REVISION_NAME

logger = logging.getLogger(__name__)


class CmdPLot(CmdBase):
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


class CmdPlotShow(CmdPLot):
    def _revisions(self):
        return None


class CmdPlotDiff(CmdPLot):
    def _revisions(self):
        revisions = self.args.revisions or []
        if len(revisions) <= 1:
            if len(revisions) == 0 and self.repo.scm.is_dirty():
                revisions.append("HEAD")
            revisions.append(WORKSPACE_REVISION_NAME)
        return revisions


def add_parser(subparsers, parent_parser):
    PLOT_HELP = (
        "Generating plots for continuous metrics stored in structured files "
        "(JSON, CSV, TSV)."
    )

    plot_parser = subparsers.add_parser(
        "plot",
        parents=[parent_parser],
        description=append_doc_link(PLOT_HELP, "plot"),
        help=PLOT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plot_subparsers = plot_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc plot CMD --help` to display command-specific help.",
    )

    fix_subparsers(plot_subparsers)

    SHOW_HELP = "Generate a plot image file from a continuous metrics file."
    plot_show_parser = plot_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        description=append_doc_link(SHOW_HELP, "plot/show"),
        help=SHOW_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plot_show_parser.add_argument(
        "-t",
        "--template",
        nargs="?",
        default=None,
        help="File to be injected with data.",
    )
    plot_show_parser.add_argument(
        "-f", "--file", default=None, help="Name of the generated file."
    )
    plot_show_parser.add_argument(
        "-s",
        "--select",
        default=None,
        help="Choose which field(s) or JSONPath to include in the plot.",
    )
    plot_show_parser.add_argument(
        "-x", default=None, help="Field name for x axis."
    )
    plot_show_parser.add_argument(
        "-y", default=None, help="Field name for y axis."
    )
    plot_show_parser.add_argument(
        "--stdout",
        action="store_true",
        default=False,
        help="Print plot specification to stdout.",
    )
    plot_show_parser.add_argument(
        "--no-csv-header",
        action="store_true",
        default=False,
        help="Required when CSV or TSV datafile does not have a header.",
    )
    plot_show_parser.add_argument(
        "--no-html",
        action="store_true",
        default=False,
        help="Do not wrap Vega plot JSON with HTML.",
    )
    plot_show_parser.add_argument("--title", default=None, help="Plot title.")
    plot_show_parser.add_argument("--xlab", default=None, help="X axis title.")
    plot_show_parser.add_argument("--ylab", default=None, help="Y axis title.")
    plot_show_parser.add_argument(
        "datafile",
        nargs="?",
        default=None,
        help="Continuous metrics file to visualize.",
    )
    plot_show_parser.set_defaults(func=CmdPlotShow)

    PLOT_DIFF_HELP = (
        "Plot continuous metrics differences between commits in the DVC "
        "repository, or between the last commit and the workspace."
    )
    plot_diff_parser = plot_subparsers.add_parser(
        "diff",
        parents=[parent_parser],
        description=append_doc_link(PLOT_DIFF_HELP, "plot/diff"),
        help=PLOT_DIFF_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plot_diff_parser.add_argument(
        "-t",
        "--template",
        nargs="?",
        default=None,
        help="File to be injected with data.",
    )
    plot_diff_parser.add_argument(
        "-d",
        "--datafile",
        nargs="?",
        default=None,
        help="Continuous metrics file to visualize.",
    )
    plot_diff_parser.add_argument(
        "-f", "--file", default=None, help="Name of the generated file."
    )
    plot_diff_parser.add_argument(
        "-s",
        "--select",
        default=None,
        help="Choose which field(s) or JSONPath to include in the plot.",
    )
    plot_diff_parser.add_argument(
        "-x", default=None, help="Field name for x axis."
    )
    plot_diff_parser.add_argument(
        "-y", default=None, help="Field name for y axis."
    )
    plot_diff_parser.add_argument(
        "--stdout",
        action="store_true",
        default=False,
        help="Print plot specification to stdout.",
    )
    plot_diff_parser.add_argument(
        "--no-csv-header",
        action="store_true",
        default=False,
        help="Provided CSV ot TSV datafile does not have a header.",
    )
    plot_diff_parser.add_argument(
        "--no-html",
        action="store_true",
        default=False,
        help="Do not wrap Vega plot JSON with HTML.",
    )
    plot_diff_parser.add_argument("--title", default=None, help="Plot title.")
    plot_diff_parser.add_argument("--xlab", default=None, help="X axis title.")
    plot_diff_parser.add_argument("--ylab", default=None, help="Y axis title.")
    plot_diff_parser.add_argument(
        "revisions",
        nargs="*",
        default=None,
        help="Git revisions to plot from",
    )
    plot_diff_parser.set_defaults(func=CmdPlotDiff)
