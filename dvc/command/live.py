import argparse
import logging

from dvc.command import completion
from dvc.command.base import CmdBase
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class CmdLive(CmdBase):
    UNINITIALIZED = True

    def run(self):
        try:
            self.repo.dvclive.summarize(self.args.target, self.args.rev)
        except DvcException:
            logger.exception("")
            return 1

        return 0


def add_parser(subparsers, parent_parser):
    # LOGS_HELP = "Generating logs summary for dvclive."

    logs_parser = subparsers.add_parser(
        "live",
        parents=[parent_parser],
        formatter_class=argparse.RawDescriptionHelpFormatter,
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
