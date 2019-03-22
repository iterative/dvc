from __future__ import unicode_literals

import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase


class CmdUnprotect(CmdBase):
    def run(self):
        for target in self.args.targets:
            try:
                self.repo.unprotect(target)
            except DvcException:
                msg = "failed to unprotect '{}'".format(target)
                logger.error(msg)
                return 1
        return 0


def add_parser(subparsers, parent_parser):
    UNPROTECT_HELP = (
        "Unprotect data file/directory.\n"
        "documentation: https://man.dvc.org/unprotect"
    )
    unprotect_parser = subparsers.add_parser(
        "unprotect",
        parents=[parent_parser],
        description=UNPROTECT_HELP,
        help=UNPROTECT_HELP,
    )
    unprotect_parser.add_argument(
        "targets", nargs="+", help="Data files/directory."
    )
    unprotect_parser.set_defaults(func=CmdUnprotect)
