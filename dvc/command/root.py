import argparse
import logging

from dvc.command.base import append_doc_link
from dvc.command.base import CmdBaseNoRepo
from dvc.utils import relpath

logger = logging.getLogger(__name__)


class CmdRoot(CmdBaseNoRepo):
    def run(self):
        from dvc.repo import Repo

        logger.info(relpath(Repo.find_root()))
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
