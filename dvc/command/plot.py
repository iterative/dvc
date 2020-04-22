import argparse
import logging
import os

from dvc.command.base import append_doc_link, CmdBase, fix_subparsers
from dvc.exceptions import DvcException
from dvc.repo.plot.data import WORKSPACE_REVISION_NAME
from dvc.utils import format_link

logger = logging.getLogger(__name__)


class CmdPLot(CmdBase):
    def _revisions(self):
        raise NotImplementedError

    def run(self):

        fields = None
        path = None
        if self.args.filter:
            if self.args.filter.startswith("$"):
                path = self.args.filter
            else:
                fields = set(self.args.filter.split(","))
        try:
            result = self.repo.plot(
                datafile=self.args.datafile,
                template=self.args.template,
                revisions=self._revisions(),
                fname=self.args.file,
                fields=fields,
                path=path,
                embed=not self.args.show_json,
            )
        except DvcException:
            logger.exception("")
            return 1
        logger.info(
            "file://{}".format(os.path.join(self.repo.root_dir, result))
        )
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
        "For visualisation of metrics stored in structured files ("
        "json, csv, tsv)."
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

    SHOW_HELP = "Visualize target dvct file using {}.".format(
        format_link("https://vega.github.io")
    )
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
        "datafile", nargs="?", default=None, help="Data to be visualized."
    )
    plot_show_parser.add_argument(
        "-f", "--file", help="Name of the generated file."
    )
    plot_show_parser.add_argument(
        "--show-json",
        action="store_true",
        default=False,
        help="Do not wrap vega plot json with HTML.",
    )
    plot_show_parser.add_argument(
        "--filter",
        default=None,
        help="Choose which fileds or jsonpath to put into plot.",
    )
    plot_show_parser.set_defaults(func=CmdPlotShow)

    PLOT_DIFF_HELP = (
        "Plot changes in metrics between commits"
        " in the DVC repository, or between a commit and the workspace."
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
        help=("File to be injected with data."),
    )
    plot_diff_parser.add_argument(
        "-d",
        "--datafile",
        nargs="?",
        default=None,
        help="Data to be visualized.",
    )
    plot_diff_parser.add_argument(
        "-f", "--file", help="Name of the generated file."
    )
    plot_diff_parser.add_argument(
        "revisions",
        nargs="*",
        default=None,
        help="Git revisions to plot from",
    )

    plot_diff_parser.add_argument(
        "--show-json",
        action="store_true",
        default=False,
        help="Do not wrap vega plot json with HTML.",
    )
    plot_diff_parser.add_argument(
        "--filter",
        default=None,
        help="Choose which filed(s) or jsonpath to put into plot.",
    )
    plot_diff_parser.set_defaults(func=CmdPlotDiff)
