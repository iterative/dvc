import argparse
import logging

from dvc.command.base import append_doc_link
from dvc.command.base import CmdBase
from dvc.exceptions import DvcException


logger = logging.getLogger(__name__)


class CmdImportUrl(CmdBase):
    def run(self):
        try:
            self.repo.imp_url(
                self.args.url, out=self.args.out, fname=self.args.file
            )
        except DvcException:
            logger.exception(
                "failed to import {}. You could also try downloading "
                "it manually, and adding it with `dvc add`.".format(
                    self.args.url
                )
            )
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    IMPORT_HELP = (
        "Download or copy file from URL and take it under DVC control."
    )

    import_parser = subparsers.add_parser(
        "import-url",
        parents=[parent_parser],
        description=append_doc_link(IMPORT_HELP, "import-url"),
        help=IMPORT_HELP,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    import_parser.add_argument(
        "url",
        help="Supported urls:\n"
        "/path/to/file\n"
        "/path/to/directory\n"
        "C:\\\\path\\to\\file\n"
        "C:\\\\path\\to\\directory\n"
        "https://example.com/path/to/file\n"
        "s3://bucket/path/to/file\n"
        "s3://bucket/path/to/directory\n"
        "gs://bucket/path/to/file\n"
        "gs://bucket/path/to/directory\n"
        "hdfs://example.com/path/to/file\n"
        "ssh://example.com:/path/to/file\n"
        "ssh://example.com:/path/to/directory\n"
        "remote://myremote/path/to/file (see `dvc remote`)",
    )
    import_parser.add_argument(
        "out", nargs="?", help="Destination path to put files to."
    )
    import_parser.add_argument(
        "-f", "--file", help="Specify name of the DVC-file it generates."
    )
    import_parser.set_defaults(func=CmdImportUrl)
