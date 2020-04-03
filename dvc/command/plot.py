import argparse
import logging
import os

from dvc.command.base import append_doc_link, CmdBase, fix_subparsers
from dvc.exceptions import DvcException
from dvc.utils import format_link

logger = logging.getLogger(__name__)


class CmdPlotShow(CmdBase):
    def run(self):
        try:
            path = self.repo.plot(self.args.targets)
            logger.info(
                "Your can see your plot by opening {} in your "
                "browser!".format(
                    format_link(
                        "file://{}".format(
                            os.path.join(self.repo.root_dir, path)
                        )
                    )
                )
            )
        except DvcException:
            logger.exception("failed to plot metrics")
        return 0


class CmdPlotDiff(CmdBase):
    def run(self):
        try:
            logger.error("Plotting diff")
            self.repo.plot(
                self.args.targets, revisions=[self.args.a_rev, self.args.b_rev]
            )

        except DvcException:
            logger.exception("failed to plot metrics diff")
            return 1

        return 0


def add_parser(subparsers, parent_parser):
    PLOT_HELP = "For visualisation"

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

    SHOW_HELP = "Visualize target metric file using {}.".format(
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
        "--template", nargs="?", help="Template file to choose."
    )
    plot_parser.add_argument(
        "target", nargs="?", help="Metric files to visualize."
    )
    plot_show_parser.set_defaults(func=CmdPlotShow)

    PLOT_DIFF_HELP = "Plot changes in metrics between commits"
    " in the DVC repository, or between a commit and the workspace."
    plot_diff_parser = plot_subparsers.add_parser(
        "diff",
        parents=[parent_parser],
        description=append_doc_link(PLOT_DIFF_HELP, "plot/diff"),
        help=PLOT_DIFF_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plot_diff_parser.add_argument(
        "a_rev", nargs="?", help="Old Git commit to plot"
    )
    plot_diff_parser.add_argument(
        "b_rev",
        nargs="?",
        help=("New Git commit to plot(defaults to the current workspace)"),
    )
    plot_diff_parser.add_argument(
        "--targets",
        nargs="*",
        help=(
            "Metric files or directories to plot for. "
            "Plots for all metric files by default."
        ),
    )
    plot_diff_parser.set_defaults(func=CmdPlotDiff)
