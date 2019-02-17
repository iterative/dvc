from __future__ import unicode_literals

import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase


class CmdAdd(CmdBase):
    def run(self):
        for target in self.args.targets:
            try:
                self.repo.add(
                    target,
                    recursive=self.args.recursive,
                    no_commit=self.args.no_commit,
                )
            except DvcException:
                logger.error("failed to add file")
                return 1
        return 0


def add_parser(subparsers, parent_parser):
    ADD_HELP = "Add files/directories to dvc."
    add_parser = subparsers.add_parser(
        "add", parents=[parent_parser], description=ADD_HELP, help=ADD_HELP
    )
    add_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Recursively add each file under the directory.",
    )
    add_parser.add_argument(
        "--no-commit",
        action="store_true",
        default=False,
        help="Don't put files/directories into cache.",
    )
    add_parser.add_argument(
        "targets", nargs="+", help="Input files/directories."
    )
    add_parser.set_defaults(func=CmdAdd)
