import argparse
import logging

from dvc.cli import completion
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class CmdImportUrl(CmdBase):
    def run(self):
        try:
            self.repo.imp_url(
                self.args.url,
                out=self.args.out,
                no_exec=self.args.no_exec,
                no_download=self.args.no_download,
                remote=self.args.remote,
                to_remote=self.args.to_remote,
                jobs=self.args.jobs,
                force=self.args.force,
                version_aware=self.args.version_aware,
            )
        except DvcException:
            logger.exception(
                (
                    "failed to import %s. You could also try downloading "
                    "it manually, and adding it with `dvc add`."
                ),
                self.args.url,
            )
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    IMPORT_HELP = "Download or copy file from URL and take it under DVC control."

    import_parser = subparsers.add_parser(
        "import-url",
        parents=[parent_parser],
        description=append_doc_link(IMPORT_HELP, "import-url"),
        help=IMPORT_HELP,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    import_parser.add_argument(
        "url",
        help=(
            "Location of the data to download. Supported URLs:\n"
            "/absolute/path/to/file/or/dir\n"
            "relative/path/to/file/or/dir\n"
            "C:\\\\path\\to\\file\\or\\dir\n"
            "https://example.com/path/to/file\n"
            "s3://bucket/key/path\n"
            "gs://bucket/path/to/file/or/dir\n"
            "hdfs://example.com/path/to/file\n"
            "ssh://example.com/absolute/path/to/file/or/dir\n"
            "remote://remote_name/path/to/file/or/dir (see `dvc remote`)"
        ),
    )
    import_parser.add_argument(
        "out", nargs="?", help="Destination path to put files to."
    ).complete = completion.DIR
    import_parser.add_argument(
        "--to-remote",
        action="store_true",
        default=False,
        help="Download it directly to the remote",
    )
    import_parser.add_argument(
        "-r",
        "--remote",
        help="Remote storage to download to",
        metavar="<name>",
    )
    no_download_exec_group = import_parser.add_mutually_exclusive_group()
    no_download_exec_group.add_argument(
        "--no-exec",
        action="store_true",
        default=False,
        help="Only create .dvc file without actually importing target data.",
    )
    no_download_exec_group.add_argument(
        "--no-download",
        action="store_true",
        default=False,
        help=(
            "Create .dvc file including target data hash value(s)"
            " but do not actually download the file(s)."
        ),
    )
    import_parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        help=(
            "Number of jobs to run simultaneously. "
            "The default value is 4 * cpu_count(). "
        ),
        metavar="<number>",
    )
    import_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Override local file or folder if exists.",
    )
    import_parser.add_argument(
        "--version-aware",
        action="store_true",
        default=False,
        help="Import using cloud versioning. Implied if the URL contains a version ID.",
    )
    import_parser.set_defaults(func=CmdImportUrl)
