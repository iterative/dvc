from __future__ import unicode_literals

import os

import dvc.logger as logger
from dvc.utils.compat import urlparse
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase


class CmdImport(CmdBase):
    def run(self):
        try:
            default_out = os.path.basename(urlparse(self.args.url).path)

            out = self.args.out or default_out

            self.repo.imp(self.args.url, out, self.args.resume)
        except DvcException:
            logger.error(
                "failed to import {}. You could also try downloading "
                "it manually and adding it with `dvc add` command.".format(
                    self.args.url
                )
            )
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    IMPORT_HELP = (
        "Import files from URL.\ndocumentation: https://man.dvc.org/import"
    )
    import_parser = subparsers.add_parser(
        "import",
        parents=[parent_parser],
        description=IMPORT_HELP,
        help=IMPORT_HELP,
    )
    import_parser.add_argument(
        "url",
        help="URL. Supported urls: "
        "'/path/to/file', "
        "'C:\\\\path\\to\\file', "
        "'https://example.com/path/to/file', "
        "'s3://bucket/path/to/file', "
        "'gs://bucket/path/to/file', "
        "'hdfs://example.com/path/to/file', "
        "'ssh://example.com:/path/to/file', "
        "'remote://myremote/path/to/file'(see "
        "`dvc remote` commands). ",
    )
    import_parser.add_argument(
        "--resume",
        action="store_true",
        default=False,
        help="Resume previously started download.",
    )
    import_parser.add_argument("out", nargs="?", help="Output.")
    import_parser.set_defaults(func=CmdImport)
