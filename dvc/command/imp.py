from __future__ import unicode_literals

import argparse
import os
import logging

from dvc.utils.compat import urlparse
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase, append_doc_link


logger = logging.getLogger(__name__)


class CmdImport(CmdBase):
    def run(self):
        try:
            default_out = os.path.basename(urlparse(self.args.url).path)

            out = self.args.out or default_out

            self.repo.imp(
                self.args.url, out, self.args.resume, fname=self.args.file
            )
        except DvcException:
            logger.exception(
                "failed to import {}. You could also try downloading "
                "it manually and adding it with `dvc add` command.".format(
                    self.args.url
                )
            )
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    IMPORT_HELP = "Download or copy files from URL and take under DVC control."

    import_parser = subparsers.add_parser(
        "import",
        parents=[parent_parser],
        description=append_doc_link(IMPORT_HELP, "import"),
        help=IMPORT_HELP,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    import_parser.add_argument(
        "url",
        help="Supported urls:\n"
        "/path/to/file\n"
        "C:\\\\path\\to\\file\n"
        "https://example.com/path/to/file\n"
        "s3://bucket/path/to/file\n"
        "gs://bucket/path/to/file\n"
        "hdfs://example.com/path/to/file\n"
        "ssh://example.com:/path/to/file\n"
        "remote://myremote/path/to/file (see `dvc remote`)",
    )
    import_parser.add_argument(
        "--resume",
        action="store_true",
        default=False,
        help="Resume previously started download.",
    )
    import_parser.add_argument(
        "out", nargs="?", help="Destination path to put files to."
    )
    import_parser.add_argument(
        "-f", "--file", help="Specify name of the DVC file it generates."
    )
    import_parser.set_defaults(func=CmdImport)
