import argparse
import logging

from dvc.exceptions import DvcException

from . import completion
from .base import CmdBaseNoRepo, append_doc_link

logger = logging.getLogger(__name__)


class CmdGetUrl(CmdBaseNoRepo):
    def run(self):
        from dvc.repo import Repo

        try:
            Repo.get_url(self.args.url, out=self.args.out)
            return 0
        except DvcException:
            logger.exception(f"failed to get '{self.args.url}'")
            return 1


def add_parser(subparsers, add_common_args):
    HELP = "Download or copy files from URL."
    parser = subparsers.add_parser(
        "get-url",
        description=append_doc_link(HELP, "get-url"),
        add_help=False,
        help=HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "url", help="See `dvc import-url -h` for full list of supported URLs."
    )
    parser.add_argument(
        "out", nargs="?", help="Destination path to put data to.",
    ).complete = completion.DIR
    parser.set_defaults(func=CmdGetUrl)
    add_common_args(parser)
