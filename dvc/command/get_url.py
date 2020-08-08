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


def add_parser(subparsers, parent_parser):
    GET_HELP = "Download or copy files from URL."
    get_parser = subparsers.add_parser(
        "get-url",
        parents=[parent_parser],
        description=append_doc_link(GET_HELP, "get-url"),
        help=GET_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_parser.add_argument(
        "url",
        help="Location of the data to download. Supported URLs:\n"
        "/absolute/path/to/file/or/dir\n"
        "relative/path/to/file/or/dir\n"
        "C:\\\\path\\to\\file\\or\\dir\n"
        "https://example.com/path/to/file\n"
        "s3://bucket/key/path\n"
        "gs://bucket/path/to/file/or/dir\n"
        "hdfs://example.com/path/to/file\n"
        "ssh://example.com/absolute/path/to/file/or/dir\n"
        "remote://remote_name/path/to/file/or/dir (see `dvc remote`)",
    )
    get_parser.add_argument(
        "out", nargs="?", help="Destination path to put data to.",
    ).complete = completion.DIR
    get_parser.set_defaults(func=CmdGetUrl)
