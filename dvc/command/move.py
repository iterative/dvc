from __future__ import unicode_literals

import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase


class CmdMove(CmdBase):
    def run(self):
        try:
            self.repo.move(self.args.src, self.args.dst)
        except DvcException:
            msg = "failed to move '{}' -> '{}'".format(
                self.args.src, self.args.dst
            )
            logger.error(msg)
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    MOVE_HELP = (
        "Rename or move a DVC controlled data file or a directory.\n"
        "documentation: https://man.dvc.org/move"
    )
    description = (
        "Rename or move a data file or a directory that "
        "is under DVC control. It renames and modifies "
        "the corresponding DVC file to reflect the changes."
    )
    help = MOVE_HELP
    move_parser = subparsers.add_parser(
        "move", parents=[parent_parser], description=description, help=help
    )
    move_parser.add_argument(
        "src", help="Source path to a data file or directory."
    )
    move_parser.add_argument("dst", help="Destination path.")
    move_parser.set_defaults(func=CmdMove)
