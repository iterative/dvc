import argparse
import logging

from dvc.command.base import append_doc_link, CmdBase

logger = logging.getLogger(__name__)


class CmdViz(CmdBase):
    def run(self):
        self.repo.viz(self.args.targets)


def add_parser(subparsers, parent_parser):
    VIZ_HELP = "Visualize target metric file using vega.io"

    viz_parser = subparsers.add_parser(
        "viz",
        parents=[parent_parser],
        description=append_doc_link(VIZ_HELP, "viz"),
        help=VIZ_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    viz_parser.add_argument(
        "targets", nargs="+", help="Metrics file to visualize."
    )
    viz_parser.set_defaults(func=CmdViz)
