from __future__ import unicode_literals

import argparse
import logging

from dvc.exceptions import DvcException
from dvc.command.base import CmdBase, append_doc_link


logger = logging.getLogger(__name__)


class CmdAdd(CmdBase):
    def run(self):
        try:
            if len(self.args.targets) > 1 and self.args.file:
                raise DvcException("can't use '--file' with multiple targets")

            for target in self.args.targets:
                self.repo.add(
                    target,
                    recursive=self.args.recursive,
                    no_commit=self.args.no_commit,
                    fname=self.args.file,
                )

        except DvcException:
            logger.exception("failed to add file")
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    ADD_HELP = "Take data files or directories under DVC control."

    add_parser = subparsers.add_parser(
        "add",
        parents=[parent_parser],
        description=append_doc_link(ADD_HELP, "add"),
        help=ADD_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
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
        "-f", "--file", help="Specify name of the DVC file it generates."
    )
    add_parser.add_argument(
        "targets", nargs="+", help="Input files/directories."
    )
    add_parser.set_defaults(func=CmdAdd)
