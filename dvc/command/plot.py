import argparse
import logging

from dvc.command.base import append_doc_link, CmdBase
from dvc.utils import format_link

logger = logging.getLogger(__name__)


class CmdPlot(CmdBase):
    def run(self):
        self.repo.plot(self.args.targets)
        return 0


def add_parser(subparsers, parent_parser):
    PLOT_HELP = "Visualize target metric file using {}.".format(
        format_link("https://vega.github.io")
    )

    plot_parser = subparsers.add_parser(
        "plot",
        parents=[parent_parser],
        description=append_doc_link(PLOT_HELP, "plot"),
        help=PLOT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plot_parser.add_argument(
        "targets", nargs="+", help="Metric files to visualize."
    )
    plot_parser.set_defaults(func=CmdPlot)
