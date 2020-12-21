import argparse
import logging
import os

from dvc.command import completion
from dvc.command.base import CmdBase
from dvc.utils.html import write

logger = logging.getLogger(__name__)


class CmdLive(CmdBase):
    UNINITIALIZED = True

    def run(self):
        metrics, plots = self.repo.live.show(self.args.target, self.args.rev)
        html_path = self.args.target + ".html"
        write(html_path, plots, metrics)
        logger.info(f"\nfile://{os.path.abspath(html_path)}")

        return 0


def add_parser(subparsers, parent_parser):
    LIVE_DESCRIPTION = (
        "Commands to visualize and compare dvclive-produced logs."
    )

    logs_parser = subparsers.add_parser(
        "live",
        parents=[parent_parser],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=LIVE_DESCRIPTION,
    )
    logs_parser.add_argument(
        "target", help="Logs dir to produce summary from",
    ).complete = completion.DIR
    logs_parser.add_argument(
        "--rev",
        nargs="*",
        default=None,
        help="Git revision (e.g. SHA, branch, tag)",
        metavar="<commit>",
    )
    logs_parser.add_argument(
        "-f", "--file", default=None, help="Name of the generated file."
    )
    logs_parser.set_defaults(func=CmdLive)
