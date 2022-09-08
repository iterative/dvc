import argparse
import logging

from dvc.cli.command import CmdBaseNoRepo
from dvc.cli.utils import append_doc_link
from dvc.exceptions import DvcException
from dvc.ui import ui

from .ls import _prettify

logger = logging.getLogger(__name__)


class CmdListUrl(CmdBaseNoRepo):
    def run(self):
        from dvc.repo import Repo

        try:
            entries = Repo.ls_url(self.args.url)
            if entries:
                entries = _prettify(entries, with_color=True)
                ui.write("\n".join(entries))
            return 0
        except DvcException:
            logger.exception("failed to list '%s'", self.args.url)
            return 1


def add_parser(subparsers, parent_parser):
    LS_HELP = "List directory contents from URL."
    get_parser = subparsers.add_parser(
        "list-url",
        aliases=["ls-url"],
        parents=[parent_parser],
        description=append_doc_link(LS_HELP, "ls-url"),
        help=LS_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_parser.add_argument(
        "url", help="See `dvc import-url -h` for full list of supported URLs."
    )
    get_parser.set_defaults(func=CmdListUrl)
