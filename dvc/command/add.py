from __future__ import unicode_literals

import argparse
import logging

from dvc.command.base import append_doc_link
from dvc.command.base import CmdBase
from dvc.exceptions import DvcException
from dvc.exceptions import RecursiveAddingWhileUsingFilename


logger = logging.getLogger(__name__)


class CmdAdd(CmdBase):
    def run(self):
        try:
            if len(self.args.targets) > 1 and self.args.file:
                raise RecursiveAddingWhileUsingFilename()

            self.repo.add(
                self.args.targets,
                recursive=self.args.recursive,
                no_commit=self.args.no_commit,
                fname=self.args.file,
            )

        except DvcException:
            logger.exception("")
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    ADD_HELP = "Track data files or directories with DVC."

    parser = subparsers.add_parser(
        "add",
        parents=[parent_parser],
        description=append_doc_link(ADD_HELP, "add"),
        help=ADD_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Recursively add files under directory targets.",
    )
    parser.add_argument(
        "--no-commit",
        action="store_true",
        default=False,
        help="Don't put files/directories into cache.",
    )
    parser.add_argument(
        "-f",
        "--file",
        help="Specify name of the DVC-file this command will generate.",
    )
    parser.add_argument(
        "targets", nargs="+", help="Input files/directories to add."
    )
    parser.set_defaults(func=CmdAdd)
