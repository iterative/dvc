import argparse
import logging

from dvc.command import choices
from dvc.command.base import CmdBase, append_doc_link
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class CmdMove(CmdBase):
    def run(self):
        try:
            self.repo.move(self.args.src_path, self.args.dst_path)
        except DvcException:
            msg = "failed to move '{}' -> '{}'".format(
                self.args.src_path, self.args.dst_path
            )
            logger.exception(msg)
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    MOVE_HELP = "Rename or move a DVC controlled data file or a directory."
    MOVE_DESCRIPTION = (
        "Rename or move a DVC controlled data file or a directory.\n"
        "It renames and modifies the corresponding DVC-file to reflect the"
        " changes."
    )

    move_parser = subparsers.add_parser(
        "move",
        parents=[parent_parser],
        description=append_doc_link(MOVE_DESCRIPTION, "move"),
        help=MOVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    move_parser.add_argument(
        "src_path",
        help="Source path to a data file or directory.",
        choices=choices.Required.FILE,
    )
    move_parser.add_argument(
        "dst_path", help="Destination path.", choices=choices.Required.FILE,
    )
    move_parser.set_defaults(func=CmdMove)
