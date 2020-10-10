import argparse
import logging

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class CmdMove(CmdBase):
    def run(self):
        try:
            self.repo.move(self.args.src, self.args.dst)
        except DvcException:
            msg = "failed to move '{}' -> '{}'".format(
                self.args.src, self.args.dst
            )
            logger.exception(msg)
            return 1
        return 0


def add_parser(subparsers, add_common_args):
    MOVE_HELP = "Rename or move a DVC controlled data file or a directory."
    MOVE_DESCRIPTION = (
        "Rename or move a DVC controlled data file or a directory.\n"
        "It renames and modifies the corresponding DVC-file to reflect the"
        " changes."
    )

    move_parser = subparsers.add_parser(
        "move",
        description=append_doc_link(MOVE_DESCRIPTION, "move"),
        add_help=False,
        help=MOVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    move_parser.add_argument(
        "src", help="Source path to a data file or directory.",
    ).complete = completion.FILE
    move_parser.add_argument(
        "dst", help="Destination path.",
    ).complete = completion.FILE
    move_parser.set_defaults(func=CmdMove)
    add_common_args(move_parser)
