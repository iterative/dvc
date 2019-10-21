from __future__ import unicode_literals

import argparse
import logging

from dvc.exceptions import DvcException
from dvc.command.base import CmdBase, append_doc_link
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
                progress=True,
            )

        except DvcException as err:
            logger.exception("{}:{}".format(type(err).__name__, err))
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    add_help = "Add data files or directories to DVC control."

    add_parser = subparsers.add_parser(
        "add",
        parents=[parent_parser],
        description=append_doc_link(add_help, "add"),
        help=add_help,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Recursively add files under directories.",
    )
    add_parser.add_argument(
        "--no-commit",
        action="store_true",
        default=False,
        help="Don't put files/directories into cache.",
    )
    add_parser.add_argument(
        "-f", "--file", help="Specify name of the generated DVC-file."
    )
    add_parser.add_argument(
        "targets", nargs="+", help="Input files/directories to add."
    )
    add_parser.set_defaults(func=CmdAdd)
