from __future__ import unicode_literals

import argparse
import os
import logging

from dvc.repo import Repo
from dvc.command.base import CmdBaseNoRepo, append_doc_link


logger = logging.getLogger(__name__)


class CmdRoot(CmdBaseNoRepo):
    def run(self):
        logger.info(os.path.relpath(Repo.find_root()))
        return 0


def add_parser(subparsers, parent_parser):
    ROOT_HELP = "Relative path to the repository's directory."
    root_parser = subparsers.add_parser(
        "root",
        parents=[parent_parser],
        description=append_doc_link(ROOT_HELP, "root"),
        help=ROOT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    root_parser.set_defaults(func=CmdRoot)
