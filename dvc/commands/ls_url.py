import argparse
import logging

from dvc.cli.command import CmdBaseNoRepo
from dvc.cli.utils import append_doc_link
from dvc.ui import ui

from .ls import _prettify

logger = logging.getLogger(__name__)


class CmdListUrl(CmdBaseNoRepo):
    def run(self):
        from dvc.repo import Repo

        entries = Repo.ls_url(self.args.url, recursive=self.args.recursive)
        if entries:
            entries = _prettify(entries, with_color=True)
            ui.write("\n".join(entries))
        return 0


def add_parser(subparsers, parent_parser):
    LS_HELP = "List directory contents from URL."
    lsurl_parser = subparsers.add_parser(
        "list-url",
        aliases=["ls-url"],
        parents=[parent_parser],
        description=append_doc_link(LS_HELP, "list-url"),
        help=LS_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    lsurl_parser.add_argument(
        "url", help="See `dvc import-url -h` for full list of supported URLs."
    )
    lsurl_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        help="Recursively list files.",
    )
    lsurl_parser.set_defaults(func=CmdListUrl)
